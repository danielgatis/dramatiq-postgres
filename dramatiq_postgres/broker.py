import logging
import random
import threading
import time
from textwrap import dedent
from uuid import uuid4

from dramatiq.broker import Broker, Consumer, MessageProxy
from dramatiq.common import current_millis, dq_name
from dramatiq.errors import BrokerConnectionError as ConnectionError
from dramatiq.message import Message
from dramatiq.results import Results
from psycopg import sql
from psycopg.types.json import Json

from .results import PostgresBackend
from .utils import (
    QueryManager,
    check_conn,
    getconn,
    make_pool,
    raise_connection_error,
    retry_pg,
    tidy4json,
    transaction,
)

logger = logging.getLogger(__name__)

# Cap the number of messages claimed in a single round trip, whatever the
# consumer prefetch. Keeps a single consumer from draining a burst alone.
CLAIM_BATCH_MAX = 256


def purge(curs, max_age="30 days"):
    # Delete old messages. Returns deleted messages.

    curs.execute(QUERIES.PURGE, (max_age,))
    return curs.rowcount


class Notifier(threading.Thread):
    # Single LISTEN connection per process, shared by every consumer. Converts
    # Postgres notifies into threading.Event wakeups. Purely a latency
    # optimization: consumers poll anyway, so a missed notify only costs one
    # poll interval.

    def __init__(self, pool):
        super().__init__(daemon=True, name="dramatiq-postgres-notifier")
        self.pool = pool
        self.lock = threading.Lock()
        self.subscriptions = {}  # channel -> set of Events.
        self.dirty = threading.Event()  # Subscriptions changed.
        self.stopping = threading.Event()
        self.started_once = False
        self.conn = None

    def subscribe(self, channel, event):
        with self.lock:
            self.subscriptions.setdefault(channel, set()).add(event)
            self.dirty.set()
            if not self.started_once:
                self.started_once = True
                self.start()

    def unsubscribe(self, channel, event):
        with self.lock:
            events = self.subscriptions.get(channel)
            if events is None:
                return
            events.discard(event)
            if not events:
                del self.subscriptions[channel]
                self.dirty.set()

    def stop(self):
        self.stopping.set()

    def run(self):
        while not self.stopping.is_set():
            try:
                conn = self.ensure_conn()
                for notify in conn.notifies(timeout=2):
                    with self.lock:
                        events = set(
                            self.subscriptions.get(notify.channel, ())
                        )
                    for event in events:
                        event.set()
                    if self.stopping.is_set() or self.dirty.is_set():
                        break
            except Exception as e:
                logger.debug("Notifier connection error: %s. Retrying.", e)
                self.drop_conn()
                self.stopping.wait(1)
        self.drop_conn()

    def ensure_conn(self):
        if self.conn is not None and not self.dirty.is_set():
            return check_conn(self.conn)

        if self.conn is None:
            self.conn = getconn(self.pool)
            self.conn.autocommit = True

        self.dirty.clear()
        with self.lock:
            channels = list(self.subscriptions)
        with self.conn.cursor() as curs:
            curs.execute("UNLISTEN *;")
            for channel in channels:
                curs.execute(
                    sql.SQL("LISTEN {};").format(sql.Identifier(channel))
                )
        logger.debug("Notifier listening on %s.", channels)
        return self.conn

    def drop_conn(self):
        if self.conn is None:
            return
        try:
            # A closed connection is discarded by the pool and replaced.
            self.conn.close()
            self.pool.putconn(self.conn)
        except Exception:
            pass
        self.conn = None


class PostgresBroker(Broker):
    def __init__(
        self,
        *,
        pool=None,
        url="",
        results=True,
        schema=None,
        prefix=None,
        listen=True,
        notify=True,
        poll_interval=1.0,
        heartbeat_interval=15.0,
        heartbeat_ttl=60.0,
        maintenance_interval=30.0,
        purge_maxage="30 days",
        **kw,
    ):
        super(PostgresBroker, self).__init__(**kw)
        if pool and url:
            raise ValueError("You can't set both pool and URL!")

        self._owns_pool = not pool
        if not pool:
            self.pool = make_pool(url)
        else:
            # Receive a pool object to have an I/O less __init__.
            self.pool = pool
        self.backend = None
        if results:
            self.backend = PostgresBackend(
                pool=self.pool, schema=schema, prefix=prefix
            )
            self.add_middleware(Results(backend=self.backend))

        QUERIES.build_queries(schema, prefix)

        self.listen = listen
        self.notify = notify
        self.poll_interval = poll_interval
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_ttl = heartbeat_ttl
        self.maintenance_interval = maintenance_interval
        self.purge_maxage = purge_maxage

        # One identity per broker instance, i.e. per worker process.
        self.worker_id = str(uuid4())
        # Namespace of the maintenance advisory lock, so that distinct
        # schema/prefix deployments sharing a database don't serialize.
        self.maintenance_ns = f"{schema or 'dramatiq'}.{prefix or ''}queue"

        self.notifier = Notifier(self.pool) if listen else None
        self._heartbeat_lock = threading.Lock()
        self._heartbeat_at = 0.0
        self._maintenance_lock = threading.Lock()
        self._maintenance_at = 0.0

    def close(self):
        if self.notifier is not None:
            self.notifier.stop()
        if self._owns_pool:
            self.pool.close()

    def consume(self, queue_name, prefetch=1, timeout=30000):
        return PostgresConsumer(
            broker=self,
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

            delayed_name = dq_name(queue_name)
            self.delay_queues.add(delayed_name)
            self.emit_after("declare_delay_queue", delayed_name)

    @retry_pg
    def enqueue(self, message, *, delay=None):
        self.emit_before("enqueue", message, delay)
        if delay:
            message = message.copy(queue_name=dq_name(message.queue_name))
            message.options["eta"] = current_millis() + delay

        q = message.queue_name
        query = QUERIES.ENQUEUE if self.notify else QUERIES.ENQUEUE_QUIET
        insert = (
            query,
            (
                q,
                message.message_id,
                Json(tidy4json(message)),
                delay or 0,
            ),
        )

        logger.debug("Upserting %s in queue %s.", message.message_id, q)
        with transaction(self.pool) as curs:
            curs.execute(*insert)
        self.emit_after("enqueue", message, delay)
        return message

    def maybe_heartbeat(self, conn):
        # Upsert this process' worker row at most every heartbeat_interval.
        # Must run before the first claim so the reaper never sees a message
        # owned by an unknown worker.
        now = time.monotonic()
        with self._heartbeat_lock:
            if self._heartbeat_at and (
                now - self._heartbeat_at < self.heartbeat_interval
            ):
                return
            self._heartbeat_at = now

        with transaction(conn) as curs:
            curs.execute(QUERIES.HEARTBEAT, (self.worker_id,))

    def maybe_maintenance(self, conn):
        # Queue upkeep: requeue messages of dead workers, drop stale worker
        # rows, purge expired results and old done/rejected messages. Guarded
        # by an advisory lock so a single worker per deployment does the work.
        now = time.monotonic()
        with self._maintenance_lock:
            if self._maintenance_at and now < self._maintenance_at:
                return
            jitter = 0.5 + random.random()
            self._maintenance_at = now + self.maintenance_interval * jitter

        with transaction(conn) as curs:
            curs.execute(QUERIES.MAINTENANCE_LOCK, (self.maintenance_ns,))
            (locked,) = curs.fetchone()
            if not locked:
                return

            curs.execute(QUERIES.REAP, (self.heartbeat_ttl,))
            if curs.rowcount:
                logger.info(
                    "Requeued %d messages from dead workers.", curs.rowcount
                )
            curs.execute(QUERIES.CLEAN_WORKERS, (self.heartbeat_ttl * 5,))
            curs.execute(QUERIES.PURGE_RESULTS)
            if self.purge_maxage is not None:
                purge(curs, self.purge_maxage)


class PostgresConsumer(Consumer):
    def __init__(self, *, broker, queue_name, prefetch, timeout, **kw):
        self.broker = broker
        self.pool = broker.pool
        self._consume_conn = None
        self.buffer = []
        self.in_processing = set()
        self.queue_name = queue_name
        self.prefetch = prefetch
        self.timeout_s = timeout / 1000
        self.wakeup = threading.Event()
        self.channel = f"dramatiq.{queue_name}.enqueue"
        if broker.notifier is not None:
            broker.notifier.subscribe(self.channel, self.wakeup)

    @raise_connection_error
    def __next__(self):
        conn = self.get_consume_conn()
        self.broker.maybe_heartbeat(conn)
        self.broker.maybe_maintenance(conn)

        if not self.buffer:
            capacity = self.prefetch - len(self.in_processing)
            if capacity > 0:
                self.claim(min(capacity, CLAIM_BATCH_MAX))

        if self.buffer:
            message = self.buffer.pop(0)
            self.in_processing.add(message.message_id)
            return MessageProxy(message)

        # Nothing claimable, or prefetch exhausted. Block until an enqueue
        # notify, an ack freeing capacity, or the poll interval elapses.
        self.wakeup.clear()
        self.wakeup.wait(min(self.timeout_s, self.broker.poll_interval))

    @raise_connection_error
    def claim(self, limit):
        with transaction(self.get_consume_conn()) as curs:
            curs.execute(
                QUERIES.CLAIM,
                (self.queue_name, limit, self.broker.worker_id),
            )
            rows = curs.fetchall()

        for (payload,) in rows:
            self.buffer.append(Message.decode(payload.encode("utf-8")))
        if rows:
            logger.debug(
                "Claimed %d messages in queue %s.", len(rows), self.queue_name
            )

    @raise_connection_error
    def ack(self, message):
        # This function is executed in worker thread!

        with transaction(self.pool) as curs:
            channel = f"dramatiq.{message.queue_name}.ack"
            logger.debug(
                "Notifying %s for ACK %s.", channel, message.message_id
            )
            # dramatiq always ack a message, even if it has been requeued by
            # the Retries middleware. Thus, only delete message in state
            # `consumed`.
            curs.execute(
                QUERIES.ACK,
                (
                    message.message_id,
                    message.queue_name,
                    channel,
                    message.message_id,
                ),
            )
        self.in_processing.discard(message.message_id)
        self.wakeup.set()

    @raise_connection_error
    def nack(self, message):
        # This function is executed in worker thread.

        with transaction(self.pool) as curs:
            # Use the same channel as ack. Actually means done.
            channel = f"dramatiq.{message.queue_name}.ack"
            logger.debug(
                "Notifying %s for NACK %s.", channel, message.message_id
            )
            payload = tidy4json(message)
            curs.execute(
                QUERIES.NACK,
                (
                    Json(payload),
                    message.message_id,
                    message.queue_name,
                    channel,
                    message.message_id,
                ),
            )
        self.in_processing.discard(message.message_id)
        self.wakeup.set()

    def close(self):
        if self.broker.notifier is not None:
            self.broker.notifier.unsubscribe(self.channel, self.wakeup)

        if self._consume_conn:
            self.pool.putconn(self._consume_conn)
            self._consume_conn = None

    def get_consume_conn(self):
        # Ensure connection used for message consumption is steady.
        if self._consume_conn is not None:
            try:
                check_conn(self._consume_conn)
            except ConnectionError:
                logger.info("Connection closed. Reconnecting...")
                self.pool.putconn(self._consume_conn)
                self._consume_conn = None

        if self._consume_conn is None:
            logger.debug("Asking new connection for message consumption.")
            self._consume_conn = getconn(self.pool)

        return self._consume_conn

    @raise_connection_error
    def requeue(self, messages):
        messages = list(messages)
        if not len(messages):
            return

        logger.debug("Batch update of messages for requeue.")
        with transaction(self.get_consume_conn()) as curs:
            curs.execute(QUERIES.REQUEUE, ([m.message_id for m in messages],))


QUERIES = QueryManager(
    dict(
        ACK=dedent("""\
        WITH acked AS (
            DELETE FROM {schema}.{tablename}
            WHERE message_id = %s
                AND queue_name = %s
                AND "state" = 'consumed'
            RETURNING message
        )
        SELECT
            pg_notify(%s,
                CASE WHEN octet_length(message::text) >= 8000
                THEN jsonb_build_object('message_id', %s::text)::text
                ELSE message::text
                END
            )
        FROM acked;
        """),
        CLAIM=dedent("""\
        WITH next AS (
            SELECT message_id
            FROM {schema}.{tablename}
            WHERE queue_name = %s
                AND "state" = 'queued'
                AND available_at <= NOW()
            ORDER BY available_at, position
            LIMIT %s
            FOR UPDATE SKIP LOCKED
        )
        UPDATE {schema}.{tablename} AS q
            SET "state" = 'consumed',
                worker_id = %s,
                consumed_at = NOW(),
                mtime = NOW()
        FROM next
        WHERE q.message_id = next.message_id
        RETURNING q.message::text;
        """),
        ENQUEUE=dedent("""\
        WITH enqueued AS (
            INSERT INTO {schema}.{tablename}
            (queue_name, message_id, "state", message, available_at)
            VALUES (%s, %s, 'queued', %s,
                    NOW() + make_interval(secs => %s / 1000.0))
            ON CONFLICT (message_id)
                DO UPDATE SET
                    "state" = 'queued',
                    message = EXCLUDED.message,
                    queue_name = EXCLUDED.queue_name,
                    available_at = EXCLUDED.available_at,
                    worker_id = NULL,
                    consumed_at = NULL,
                    mtime = NOW()
            RETURNING queue_name
        )
        SELECT pg_notify('dramatiq.' || queue_name || '.enqueue', '')
        FROM enqueued;
        """),
        ENQUEUE_QUIET=dedent("""\
        INSERT INTO {schema}.{tablename}
        (queue_name, message_id, "state", message, available_at)
        VALUES (%s, %s, 'queued', %s,
                NOW() + make_interval(secs => %s / 1000.0))
        ON CONFLICT (message_id)
            DO UPDATE SET
                "state" = 'queued',
                message = EXCLUDED.message,
                queue_name = EXCLUDED.queue_name,
                available_at = EXCLUDED.available_at,
                worker_id = NULL,
                consumed_at = NULL,
                mtime = NOW();
        """),
        NACK=dedent("""\
        WITH updated AS (
            UPDATE {schema}.{tablename}
                SET "state" = 'rejected', message = %s,
                    worker_id = NULL, mtime = NOW()
            WHERE message_id = %s
                AND queue_name = %s
                AND state <> 'rejected'
            RETURNING message
        )
        SELECT
            pg_notify(%s,
                CASE WHEN octet_length(message::text) >= 8000
                THEN jsonb_build_object('message_id', %s::text)::text
                ELSE message::text
                END
            )
        FROM updated;
        """),
        REQUEUE=dedent("""\
        UPDATE {schema}.{tablename}
            SET "state" = 'queued', worker_id = NULL,
                consumed_at = NULL, mtime = NOW()
        WHERE message_id = ANY(%s::uuid[]) AND "state" = 'consumed';
        """),
        HEARTBEAT=dedent("""\
        INSERT INTO {schema}.{workertable} (worker_id, heartbeat_at)
        VALUES (%s, NOW())
        ON CONFLICT (worker_id) DO UPDATE SET heartbeat_at = NOW();
        """),
        MAINTENANCE_LOCK=dedent("""\
        SELECT pg_try_advisory_xact_lock(
            hashtext(%s), hashtext('dramatiq-postgres'));
        """),
        REAP=dedent("""\
        UPDATE {schema}.{tablename} AS q
            SET "state" = 'queued', worker_id = NULL,
                consumed_at = NULL, mtime = NOW()
        WHERE q."state" = 'consumed'
            AND (q.worker_id IS NULL
                OR NOT EXISTS (
                    SELECT 1 FROM {schema}.{workertable} AS w
                    WHERE w.worker_id = q.worker_id
                        AND w.heartbeat_at >
                            NOW() - make_interval(secs => %s)));
        """),
        CLEAN_WORKERS=dedent("""\
        DELETE FROM {schema}.{workertable}
        WHERE heartbeat_at < NOW() - make_interval(secs => %s);
        """),
        PURGE_RESULTS=dedent("""\
        DELETE FROM {schema}.{resulttable}
        WHERE expires_at IS NOT NULL AND expires_at <= NOW();
        """),
        PURGE=dedent("""\
        DELETE FROM {schema}.{tablename}
        WHERE "state" = 'rejected'
        AND mtime <= (NOW() - %s::interval);
        """),
    )
)
