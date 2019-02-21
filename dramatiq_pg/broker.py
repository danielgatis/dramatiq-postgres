import json
import logging
import select
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
        self.pool = pool
        self.queue_name = queue_name
        self.notifies = []
        self.listen_conn = None

    def __next__(self):
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
            self.listen()

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
            channel = quote_ident(f"dramatiq.{self.queue_name}.ack", curs)
            curs.execute(f"NOTIFY {channel}, %s;", (message.message_id,))

    def close(self):
        if self.listen_conn:
            self.pool.putconn(self.listen_conn)
            self.listen_conn = None

    def consume_one(self, message):
        # Race to process this message.
        with transaction(self.pool) as curs:
            curs.execute(dedent("""\
            UPDATE dramatiq.queue
            SET "state" = 'consumed'
            WHERE message_id = %s AND "state" = 'queued';
            """), (message.message_id,))
            # If no row was updated, this mean another worker has consumed it.
            return 1 == curs.rowcount

    def listen(self):
        if self.listen_conn is None:
            self.listen_conn = self.start_listening()

        while not self.notifies:
            rlist, *_ = select.select([self.listen_conn], [], [], 300)
            if not rlist:
                continue  # Loop on timeout
            self.listen_conn.poll()
            self.notifies += self.listen_conn.notifies
            self.listen_conn.notifies[:] = []

    def start_listening(self):
        conn = self.pool.getconn()
        # This is for NOTIFY consistency, according to psycopg2 doc.
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        channel = quote_ident(f"dramatiq.{self.queue_name}.enqueue", conn)
        with conn.cursor() as curs:
            logger.debug("Listening on channel %s.", channel)
            curs.execute(f"LISTEN {channel};")
        return conn
