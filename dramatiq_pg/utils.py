import functools
import json
import logging
import select
from contextlib import ExitStack, contextmanager
from urllib.parse import parse_qsl, urlparse

import tenacity
from dramatiq import Message, MessageProxy, get_encoder
from dramatiq.errors import ConnectionError
from psycopg2 import InterfaceError, OperationalError, __libpq_version__
from psycopg2.errors import AdminShutdown, DatabaseError
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extensions import quote_ident as pq_quote_ident
from psycopg2.pool import PoolError, ThreadedConnectionPool

logger = logging.getLogger(__name__)


DISCONNECT_ERRORS = (
    AdminShutdown,
    InterfaceError,
    OperationalError,
)


retry_pg = tenacity.retry(
    retry=tenacity.retry_if_exception_type(
        DISCONNECT_ERRORS
        + (
            ConnectionError,
            DatabaseError,
        )
    ),
    reraise=True,
    wait=tenacity.wait_random_exponential(multiplier=1, max=30),
    stop=tenacity.stop_after_attempt(10),
    before_sleep=tenacity.before_sleep_log(logger, logging.INFO),
)


def check_conn(conn):
    try:
        conn.poll()
    except DISCONNECT_ERRORS as e:
        if not conn.closed:
            logger.debug("Closing connexion due to error: %s", e)
            try:
                conn.close()
            except Exception as close_e:
                logger.debug("Failed to close connexion: %s", close_e)
        raise ConnectionError(str(e)) from None
    return conn


@retry_pg
def getconn(pool):
    # Get a reliable connection to Postgres.
    conn = pool.getconn()
    try:
        check_conn(conn)
    except ConnectionError:
        pool.putconn(conn)
        raise  # Let tenacity control retry.
    return conn


@retry_pg
def make_pool(url, maxconn=16):
    if isinstance(url, str):
        parts = urlparse(url)
        kwargs = dict(parse_qsl(parts.query))
        parts = parts._replace(query="")
        conninfo = parts.geturl()
    else:
        conninfo = ""
        kwargs = dict(url)

    kwargs.setdefault("application_name", "dramatiq-pg")
    kwargs.setdefault("keepalives", "1")
    kwargs.setdefault("keepalives_count", "2")
    kwargs.setdefault("keepalives_idle", "5")
    kwargs.setdefault("keepalives_interval", "2")

    if __libpq_version__ >= 120000:
        kwargs.setdefault("tcp_user_timeout", "10000")

    maxconn = int(kwargs.pop("maxconn", maxconn))
    minconn = int(kwargs.pop("minconn", maxconn))  # Default to maxconn.

    pool = ThreadedConnectionPool(0, maxconn, conninfo, **kwargs)
    pool.minconn = minconn
    return pool


@contextmanager
def pool_sanitizer(pool):
    # When a connection is broken, other connection in the pool are likely
    # broken as well. This context manager walk unused connection in the pool
    # to check their health and immediatly clean unusable connections. This
    # avoid exhausting retry attempts by looping on broken connections in the
    # pool.

    try:
        yield pool
    except DISCONNECT_ERRORS:
        for _ in range(pool.minconn):
            try:
                conn = pool.getconn()
            except PoolError:
                # Pool exhausted. Other connection will be handled by their
                # holder.
                break

            # Pen test connection with an explicit query.
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT 'dramatiq-pg conn test'")
            except DISCONNECT_ERRORS as e:
                logger.debug("Bad connection detected: %s", e)
                if not conn.closed:
                    conn.close()
            pool.putconn(conn)

        # Re raise original error for business or retry logic.
        raise


def raise_connection_error(fn):
    # Raises Dramatiq connection error on Psycopg2 error

    @functools.wraps(fn)
    def wrapper(*a, **kw):
        try:
            return fn(*a, **kw)
        except OperationalError as e:
            raise ConnectionError(str(e))

    return wrapper


def quote_ident(raw):
    # Quote an SQL identifier, free from a connection object.
    return '"%s"' % raw.replace('"', '""')


@contextmanager
def transaction(conn_or_pool, listen=None):
    # Manage the connection, transaction and cursor from a connection pool.
    new_conn = hasattr(conn_or_pool, "getconn")
    with ExitStack() as defer:
        if new_conn:
            defer.enter_context(pool_sanitizer(conn_or_pool))
            conn = getconn(conn_or_pool)
            defer.callback(conn_or_pool.putconn, conn)
        else:
            conn = conn_or_pool

        if listen:
            # This is for NOTIFY consistency, according to psycopg2 doc.
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        with conn:  # Wraps in a transaction.
            with conn.cursor() as curs:
                if listen:
                    channel = pq_quote_ident(listen, conn)
                    curs.execute(f"LISTEN {channel};")
                yield curs


def wait_for_notifies(conn, timeout=1):
    rlist, *_ = select.select([conn], [], [], timeout)
    check_conn(conn)  # Pools connection and notifies on the way.
    notifies = conn.notifies[:]
    if notifies:
        logger.debug("Received %d Postgres notifies.", len(conn.notifies))
        conn.notifies[:] = []
    return notifies


class QueryManager:
    def __init__(self, queries, schema="dramatiq", prefix=""):
        self.queries = queries
        self.schema = schema
        self.prefix = prefix
        self.build_queries(schema, prefix)

    def build_queries(self, schema=None, prefix=None):
        if not (schema or prefix):
            return

        for name, sql in self.queries.items():
            setattr(
                self,
                name,
                sql.format(
                    schema=quote_ident(schema),
                    tablename=quote_ident(prefix + "queue"),
                ),
            )


def tidy4json(data):
    if isinstance(data, (Message, MessageProxy)):
        # Translate python data into decoded json.
        # Encode message using Dramatiq encoder. But immediatly decode it as
        # standard json to send native json to PostgreSQL.
        # e.g. date formating problem
        return json.loads(data.encode())
    else:
        return json.loads(get_encoder().encode(data))
