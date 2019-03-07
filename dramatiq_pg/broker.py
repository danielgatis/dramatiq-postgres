import json
import logging
import select
from random import randint
from contextlib import contextmanager
from textwrap import dedent

from dramatiq.broker import (
    Broker,
    Consumer,
    MessageProxy,
)
from dramatiq.message import Message
from psycopg2.extensions import (
    ISOLATION_LEVEL_AUTOCOMMIT,
    Notify,
    quote_ident,
)
from psycopg2.extras import Json


logger = logging.getLogger(__name__)


@contextmanager
def transaction(pool):
    # Manage the connection, transaction and cursor from a connection pool.
    conn = pool.getconn()
    try:
        with conn:  # Wraps in a transaction.
            with conn.cursor() as curs:
                yield curs
    finally:
        pool.putconn(conn)


def purge(curs, max_age='30 days'):
    # Delete old messages. Returns deleted messages.

    curs.execute(dedent("""\
    DELETE FROM dramatiq.queue
    WHERE "state" IN ('done', 'rejected')
      AND mtime <= (NOW() - interval %s);
    """), (max_age,))
    return curs.rowcount


class PostgresBroker(Broker):
    def __init__(self, *, pool, **kw):
        super(PostgresBroker, self).__init__(**kw)
        # Receive a pool object to have an I/O less __init__.
        self.pool = pool

    def consume(self, queue_name, prefetch=1, timeout=30000):
        return PostgresConsumer(
            pool=self.pool,
            queue_name=queue_name,
            prefetch=prefetch,
            timeout=timeout,
        )

    def declare_queue(self, queue_name):
        if queue_name not in self.queues:
            self.emit_before("declare_queue", queue_name)
            self.queues[queue_name] = True
            # Actually do nothing in Postgres since all queues are stored in
            # the same table.
            self.emit_after("declare_queue", queue_name)

    def enqueue(self, message, *, delay=None):
        q = message.queue_name
        insert = (dedent("""\
        WITH enqueued AS (
          INSERT INTO dramatiq.queue (queue_name, message_id, "state", message)
          VALUES (%s, %s, 'queued', %s)
          ON CONFLICT (message_id)
            DO UPDATE SET "state" = 'queued', message = EXCLUDED.message
          RETURNING queue_name, message
        )
        SELECT
          pg_notify('dramatiq.' || queue_name || '.enqueue', message::text)
        FROM enqueued;
        """), (q, message.message_id, Json(message.asdict())))

        with transaction(self.pool) as curs:
            logger.debug("Upserting %s in %s.", message.message_id, q)
            curs.execute(*insert)


class PostgresConsumer(Consumer):
    def __init__(self, *, pool, queue_name, **kw):
        prefix = "dramatiq." + queue_name
        self.ack_channel = prefix + ".ack"
        self.enqueue_channel = prefix + ".enqueue"
        self.listen_conn = None
        self.notifies = []
        self.pool = pool
        self.queue_name = queue_name

    def __next__(self):
        self.auto_purge()
        while True:
            # Start by processing already fetched notifies.
            while self.notifies:
                notify = self.notifies.pop(0)
                payload = json.loads(notify.payload)
                message = Message(**payload)
                mid = message.message_id
                if self.consume_one(message):
                    logger.debug("Consumed message %s.", mid)
                    return MessageProxy(message)
                else:
                    logger.debug("Message %s already consumed.", mid)

            # Notify list is empty, listen for more.
            self.wait_for_notify()

    def ack(self, message):
        with transaction(self.pool) as curs:
            # dramatiq always ack a message, even if it has been requeued by
            # the Retries middleware. Thus, only update message in state
            # `consumed`.
            curs.execute(dedent("""\
            UPDATE dramatiq.queue
            SET "state" = 'done'
            WHERE message_id = %s AND "state" = 'consumed'
            """), (message.message_id,))
            # Always notify ack, even if message has been requeued. ack just
            # mean message leaved state consumed.
            channel = quote_ident(self.ack_channel, curs)
            curs.execute(f"NOTIFY {channel}, %s;", (message.message_id,))

    def auto_purge(self):
        # Automatically purge messages every 100k messages.
        if 0 == randint(0, 100_000):
            with transaction(self.pool) as curs:
                deleted = purge(curs)
            logger.info("Purged %d messages.", deleted)

    def close(self):
        if self.listen_conn:
            self.pool.putconn(self.listen_conn)
            self.listen_conn = None

    def consume_one(self, message):
        # Race to process this message.
        with transaction(self.pool) as curs:
            curs.execute(dedent("""\
            UPDATE dramatiq.queue
               SET "state" = 'consumed',
                   mtime = (NOW() AT TIME ZONE 'UTC')
             WHERE message_id = %s AND "state" = 'queued';
            """), (message.message_id,))
            # If no row was updated, this mean another worker has consumed it.
            return 1 == curs.rowcount

    def nack(self, message):
        with transaction(self.pool) as curs:
            payload = Json(message.asdict())
            curs.execute(dedent("""\
            WITH updated AS (
              UPDATE dramatiq.queue
                 SET "state" = 'rejected', message = %s
               WHERE message_id = %s AND state <> 'rejected'
              RETURNING message
            )
            SELECT
              pg_notify(%s, message::text)
            FROM updated;
            """), (payload, message.message_id, self.ack_channel))

    def recover(self):
        # Requeue old consumed message.
        #
        # In dramatiq, actor should be idempotent. In other words, it's safer
        # to retry a task rather than losing a message. Thus, when recovering,
        # we requeue message consumed for at least 3 seconds.
        #
        # To ensure full recovery, you should wait 3 seconds before restarting
        # dramatiq worker process. A task consumed in the last 3s should have
        # been consumed by another worker processus.
        logger.debug("Recover consumed message from %s.", self.queue_name)
        with self.listen_conn.cursor() as curs:
            curs.execute(dedent("""\
            UPDATE dramatiq.queue
               SET state = 'queued'
             WHERE state = 'consumed'
               AND mtime < (NOW() AT TIME ZONE 'UTC') - interval '3s';
            """), (self.queue_name,))
            logger.debug("Recovered %s messages.", curs.rowcount)

    def replay_pending_notifies(self):
        logger.debug("Querying pending messages in %s.", self.queue_name)
        with self.listen_conn.cursor() as curs:
            curs.execute(dedent("""\
            SELECT message::text
              FROM dramatiq.queue
             WHERE state = 'queued' AND queue_name = %s;
            """), (self.queue_name,))
            return [
                Notify(pid=0, channel=self.ack_channel, payload=r[0])
                for r in curs
            ]

    def start_listening(self):
        # Opens listening connection with proper configuration.

        conn = self.pool.getconn()
        # This is for NOTIFY consistency, according to psycopg2 doc.
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        channel = quote_ident(self.enqueue_channel, conn)
        with conn.cursor() as curs:
            logger.debug("Listening on channel %s.", channel)
            curs.execute(f"LISTEN {channel};")
        return conn

    def wait_for_notify(self):
        # Blocks until a notify is intercepted.

        if self.listen_conn is None:
            self.listen_conn = self.start_listening()
            self.recover()
            # We may have received a notify between LISTEN and SELECT of
            # pending messages. That's not a problem because we are able to
            # skip spurious notifies.
            self.notifies = self.replay_pending_notifies()
            logger.debug(
                "Found %s pending messages in %s.",
                len(self.notifies), self.queue_name)

        while not self.notifies:
            rlist, *_ = select.select([self.listen_conn], [], [], 300)
            if not rlist:
                continue  # Loop on timeout
            self.listen_conn.poll()
            self.notifies += self.listen_conn.notifies
            self.listen_conn.notifies[:] = []
