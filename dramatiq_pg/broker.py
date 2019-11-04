import json
import logging
import time
from queue import Empty, Queue
from random import randint
from textwrap import dedent

from dramatiq.broker import (
    Broker,
    Consumer,
    MessageProxy,
)
from dramatiq.common import current_millis, dq_name, compute_backoff
from dramatiq.message import Message
from dramatiq.results import Results
from psycopg2.extensions import (
    ISOLATION_LEVEL_AUTOCOMMIT,
    Notify,
    quote_ident,
)
from psycopg2.extras import Json

from .utils import (
    ConnectionClosed, check_conn, getconn, make_pool,
    transaction,
)
from .results import PostgresBackend
from .utils import wait_for_notifies


logger = logging.getLogger(__name__)


def purge(curs, max_age='30 days'):
    # Delete old messages. Returns deleted messages.

    curs.execute(dedent("""\
    DELETE FROM dramatiq.queue
     WHERE "state" IN ('done', 'rejected')
       AND mtime <= (NOW() - interval %s);
    """), (max_age,))
    return curs.rowcount


class PostgresBroker(Broker):
    def __init__(self, *, pool=None, url="", results=True, **kw):
        super(PostgresBroker, self).__init__(**kw)
        if pool and url:
            raise ValueError("You can't set both pool and URL!")

        if not pool:
            self.pool = make_pool(url)
        else:
            # Receive a pool object to have an I/O less __init__.
            self.pool = pool
        self.backend = None
        if results:
            self.backend = PostgresBackend(pool=self.pool)
            self.add_middleware(Results(backend=self.backend))

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
        if delay:
            message = message.copy(queue_name=dq_name(message.queue_name))
            message.options['eta'] = current_millis() + delay

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
            logger.debug("Upserting %s in queue %s.", message.message_id, q)
            curs.execute(*insert)
        return message


class PostgresConsumer(Consumer):
    def __init__(self, *, pool, queue_name, prefetch, timeout, **kw):
        self._consume_conn = None
        self._listen_conn = None
        self.notifies = []
        self.pool = pool
        self.queue_name = queue_name
        self.timeout = timeout // 1000
        self.unlock_q = Queue()
        self.in_processing = 0
        self.prefetch = prefetch
        self.misses = 0

    def __next__(self):
        # This function is executed each second.

        # First, open connexion and fetch missed notifies from table.
        if self._listen_conn is None:
            # Before reading from LISTEN, scan queue for missed messages.
            self.notifies = self.fetch_pending_notifies()
            logger.debug(
                "Found %s pending messages in queue %s.",
                len(self.notifies), self.queue_name)
        #
        if self.in_processing >= self.prefetch:
            # Wait and don't consume the message, other worker will be faster
            self.misses, backoff_ms = compute_backoff(self.misses,
                                                      max_backoff=1000)
            logger.debug(f"Too many messages in processing:"
                         f" {self.in_processing}"
                         f" sleeping {backoff_ms}")
            time.sleep(backoff_ms / 1000)
            return None

        if not self.notifies:
            # Then, fetch notifies from Pg connexion.
            self.poll_for_notify()

        if not self.notifies and randint(0, 300):
            # If notifies are consumed, randomly poll for crashed messages.
            # Since we're called each second, this condition limits polling to
            # one SELECT every five minutes of inactivity.
            self.notifies[:] = self.fetch_pending_notifies()

        # If we have some notifies, loop to find one todo.
        while self.notifies:
            notify = self.notifies.pop(0)
            payload = json.loads(notify.payload)
            message = Message(**payload)
            mid = message.message_id
            if self.consume_one(message):
                self.in_processing += 1
                return MessageProxy(message)
            else:
                logger.debug("Message %s already consumed. Skipping.", mid)

        # No message to process. Let's clean locks.
        self.purge_locks()

        # We have nothing to do, let's see if the queue needs some cleaning.
        self.auto_purge()

    def ack(self, message):
        # This function is executed in worker thread!

        with transaction(self.pool) as curs:
            channel = f"dramatiq.{message.queue_name}.ack"
            payload = Json(message.asdict())
            logger.debug(
                "Notifying %s for ACK %s.", channel, message.message_id)
            # dramatiq always ack a message, even if it has been requeued by
            # the Retries middleware. Thus, only update message in state
            # `consumed`.
            curs.execute(dedent("""\
            WITH updated AS (
              UPDATE dramatiq.queue
                 SET "state" = 'done', message = %s
               WHERE message_id = %s
                 AND state = 'consumed'
              RETURNING message
            )
            SELECT
              pg_notify(%s, message::text)
            FROM updated;
            """), (payload, message.message_id, channel))
            self.unlock_q.put_nowait(message.message_id)
        self.in_processing -= 1

    def auto_purge(self):
        # Automatically purge messages every 100k iteration. Dramatiq defaults
        # to 1s. This mean about 1 purge for 28h idle.
        if randint(0, 100_000):
            return
        logger.debug("Randomly triggering garbage collector.")
        with transaction(self.consume_conn) as curs:
            deleted = purge(curs)
        logger.info("Purged %d messages in all queues.", deleted)

    def close(self):
        if self._listen_conn:
            self.pool.putconn(self._listen_conn)
            self._listen_conn = None

        if self._consume_conn:
            self.pool.putconn(self._consume_conn)
            self._consume_conn = None

    def get_consume_conn(self):
        # Ensure connection used for message consumption is steady.
        if self._consume_conn is not None:
            try:
                check_conn(self._consume_conn)
            except ConnectionClosed:
                logger.info("Connection closed. Reconnecting...")
                self.pool.putconn(self._consum_conn)
                self._consume_conn = None

        if self._consume_conn is None:
            logger.debug("Using new connection for message consumption.")
            self._consume_conn = getconn(self.pool)

        return self._consume_conn

    def get_listen_conn(self):
        # Opens listening connection with proper configuration.
        if self._listen_conn is not None:
            try:
                return check_conn(self._listen_conn)
            except ConnectionClosed:
                logger.info("Connection closed. Reconnecting...")

        self._listen_conn = conn = getconn(self.pool)
        # This is for NOTIFY consistency, according to psycopg2 doc.
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        channel = quote_ident(f"dramatiq.{self.queue_name}.enqueue", conn)
        dq = dq_name(self.queue_name)
        dchannel = quote_ident(f"dramatiq.{dq}.enqueue", conn)
        with conn.cursor() as curs:
            logger.debug(
                "Listening on channels %s, %s.", channel, dchannel)
            curs.execute(f"LISTEN {channel}; LISTEN {dchannel};")
        return self._listen_conn

    def consume_one(self, message):
        # Race to process message.
        with transaction(self.get_consume_conn()) as curs:
            lock = hash(str(message.message_id))
            curs.execute(dedent("""\
            UPDATE dramatiq.queue
               SET "state" = 'consumed',
                   mtime = (NOW() AT TIME ZONE 'UTC')
             WHERE message_id = %s
               AND state IN ('queued', 'consumed')
               AND pg_try_advisory_lock(%s);
            """), (message.message_id, lock))
            # If no row was updated, this mean another worker has consumed it.
            if curs.rowcount:
                logger.info(
                    "Consumed %s@%s.", message.message_id, message.queue_name)
            return 1 == curs.rowcount

    def nack(self, message):
        # This function is executed in worker thread.

        with transaction(self.pool) as curs:
            # Use the same channel as ack. Actually means done.
            channel = f"dramatiq.{message.queue_name}.ack"
            logger.debug(
                "Notifying %s for NACK %s.", channel, message.message_id)
            payload = Json(message.asdict())
            curs.execute(dedent("""\
            WITH updated AS (
              UPDATE dramatiq.queue
                 SET "state" = 'rejected', message = %s
               WHERE message_id = %s
                 AND state <> 'rejected'
              RETURNING message
            )
            SELECT
              pg_notify(%s, message::text)
            FROM updated;
            """), (payload, message.message_id, channel))
            self.unlock_q.put_nowait(message.message_id)
        self.in_processing -= 1

    def fetch_pending_notifies(self):
        # Get or open connection.
        conn = self.get_listen_conn()
        # We may have received a notify between LISTEN and SELECT of pending
        # messages. That's not a problem because we are able to skip spurious
        # notifies.
        with transaction(conn) as curs:
            curs.execute(dedent("""\
            SELECT message::text
              FROM dramatiq.queue
             WHERE state IN ('queued', 'consumed')
               AND queue_name IN %s;
            """), ((self.queue_name, dq_name(self.queue_name)),))
            return [
                Notify(pid=0, channel=None, payload=r[0])
                for r in curs
            ]

    def poll_for_notify(self):
        self.notifies += wait_for_notifies(
            self.get_listen_conn(), self.timeout)

    def purge_locks(self):
        with transaction(self.get_consume_conn()) as curs:
            while True:
                try:
                    message_id = self.unlock_q.get(block=False)
                except Empty:
                    return
                lock = hash(str(message_id))
                logger.debug("Unlocking %s.", message_id)
                curs.execute(dedent("""\
                SELECT pg_advisory_unlock(%s);
                """), (lock,))
                self.unlock_q.task_done()

    def requeue(self, messages):
        messages = list(messages)
        if not len(messages):
            return

        logger.debug("Batch update of messages for requeue.")
        with transaction(self.get_consume_conn()) as curs:
            curs.execute(dedent("""\
            UPDATE dramatiq.queue
               SET state = 'queued'
            WHERE message_id IN %s;
            """), (tuple(m.message_id for m in messages),))
            # We don't bother about locks, because requeue occurs on worker
            # stop.
