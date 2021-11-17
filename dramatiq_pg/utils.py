import logging
import select
from contextlib import contextmanager
from urllib.parse import (
    parse_qsl,
    urlencode,
    urlparse,
)

from psycopg2 import DatabaseError, OperationalError
from psycopg2.extensions import (
    ISOLATION_LEVEL_AUTOCOMMIT,
    quote_ident as pq_quote_ident,
)
from psycopg2.pool import ThreadedConnectionPool
import tenacity


logger = logging.getLogger(__name__)


class ConnectionClosed(DatabaseError):
    pass


retry_pg = tenacity.retry(
    retry=tenacity.retry_if_exception_type(
      (OperationalError, ConnectionClosed),
    ),
    reraise=True,
    wait=tenacity.wait_random_exponential(multiplier=1, max=30),
    stop=tenacity.stop_after_attempt(10),
    before_sleep=tenacity.before_sleep_log(logger, logging.INFO),
)


def check_conn(conn):
    try:
        conn.poll()
    except OperationalError as e:
        logger.debug("Closing connexion due to error: %s", e)
        try:
            conn.close()
        except Exception as close_e:
            logger.debug("Failed to close connexion: %s", close_e)
        raise ConnectionClosed(str(e))
    return conn


@retry_pg
def getconn(pool):
    # Get a reliable connection to Postgres.
    conn = pool.getconn()
    try:
        check_conn(conn)
    except ConnectionClosed:
        pool.putconn(conn)
        raise  # Let tenacity control retry.
    return conn


@retry_pg
def make_pool(url, maxconn=16):
    parts = urlparse(url)
    qs = dict(parse_qsl(parts.query))
    qs.setdefault('application_name', 'dramatiq-pg')
    maxconn = int(qs.pop('maxconn', maxconn))
    minconn = int(qs.pop('minconn', maxconn))  # Default to maxconn.
    parts = parts._replace(query=urlencode(qs))
    connstring = parts.geturl()
    if connstring.startswith("?"):
        connstring = 'postgresql:///' + connstring
    if ":/?" in connstring or connstring.endswith(':/'):
        # geturl replaces :/ with :///. libpq does not accept that.
        connstring = connstring.replace(':/', ':///')
    pool = ThreadedConnectionPool(0, maxconn, connstring)
    pool.minconn = minconn
    return pool


def quote_ident(raw):
    # Quote an SQL identifier, free from a connection object.
    return '"%s"' % raw.replace('"', '""')


@contextmanager
def transaction(conn_or_pool, listen=None):
    # Manage the connection, transaction and cursor from a connection pool.
    new_conn = hasattr(conn_or_pool, 'getconn')
    if new_conn:
        conn = getconn(conn_or_pool)
    else:
        conn = conn_or_pool

    if listen:
        # This is for NOTIFY consistency, according to psycopg2 doc.
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    try:
        with conn:  # Wraps in a transaction.
            with conn.cursor() as curs:
                if listen:
                    channel = pq_quote_ident(listen, conn)
                    curs.execute(f"LISTEN {channel};")
                yield curs
    finally:
        if new_conn:
            conn_or_pool.putconn(conn)


def wait_for_notifies(conn, timeout=1):
    rlist, *_ = select.select([conn], [], [], timeout)
    check_conn(conn)  # Pools connection and notifies on the way.
    notifies = conn.notifies[:]
    if notifies:
        logger.debug("Received %d Postgres notifies.", len(conn.notifies))
        conn.notifies[:] = []
    return notifies


class QueryManager:
    def __init__(self, queries, schema="dramatiq", table="queue"):
        self.queries = queries
        self.schema = schema
        self.table = table
        self.build_queries(schema, table)

    def build_queries(self, schema=None, table=None):
        if not (schema or table):
            return

        for name, sql in self.queries.items():
            setattr(self, name, sql.format(
                schema=quote_ident(schema or self.schema),
                tablename=quote_ident(table or self.table),
            ))

    # Tell pytype/pyright this class has dynamic attributes.
    def __getattr__(self, name):
        raise AttributeError(name)
