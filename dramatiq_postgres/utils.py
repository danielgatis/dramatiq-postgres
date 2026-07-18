import functools
import json
import logging
from contextlib import ExitStack, contextmanager, nullcontext
from typing import TYPE_CHECKING
from urllib.parse import parse_qsl, urlparse

import tenacity
from dramatiq import Message, MessageProxy, get_encoder
from dramatiq.errors import BrokerConnectionError as ConnectionError
from psycopg import InterfaceError, OperationalError, pq, sql
from psycopg.errors import AdminShutdown, DatabaseError
from psycopg_pool import ConnectionPool, PoolClosed

logger = logging.getLogger(__name__)


DISCONNECT_ERRORS = (
    AdminShutdown,
    InterfaceError,
    OperationalError,
)

_RETRY_ERRORS = DISCONNECT_ERRORS + (
    ConnectionError,
    DatabaseError,
)


retry_pg = tenacity.retry(
    # A closed pool means the broker was shut down on purpose: fail fast
    # instead of retrying.
    retry=tenacity.retry_if_exception(
        lambda e: isinstance(e, _RETRY_ERRORS)
        and not isinstance(e, PoolClosed)
    ),
    reraise=True,
    wait=tenacity.wait_random_exponential(multiplier=1, max=30),
    stop=tenacity.stop_after_attempt(10),
    before_sleep=tenacity.before_sleep_log(logger, logging.INFO),
)


def check_conn(conn):
    try:
        # Reads pending input without a server round trip, erroring out if
        # the socket is dead. Received notifications land in the backlog
        # served by conn.notifies().
        conn.pgconn.consume_input()
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
        # check_conn closed it; the pool discards it and opens a fresh one.
        pool.putconn(conn)
        raise  # Let tenacity control retry.
    return conn


def make_pool(url, maxconn=16):
    if isinstance(url, str):
        parts = urlparse(url)
        kwargs = dict(parse_qsl(parts.query))
        parts = parts._replace(query="")
        conninfo = parts.geturl()
    else:
        conninfo = ""
        kwargs = dict(url)

    kwargs.setdefault("application_name", "dramatiq-postgres")
    kwargs.setdefault("keepalives", "1")
    kwargs.setdefault("keepalives_count", "2")
    kwargs.setdefault("keepalives_idle", "5")
    kwargs.setdefault("keepalives_interval", "2")

    if pq.version() >= 120000:
        kwargs.setdefault("tcp_user_timeout", "10000")

    maxconn = int(kwargs.pop("maxconn", maxconn))
    # Open connections on demand only, none upfront.
    minconn = int(kwargs.pop("minconn", 0))

    return ConnectionPool(
        conninfo,
        kwargs=kwargs,
        min_size=minconn,
        max_size=maxconn,
        open=True,
        # Broken connections are verified and replaced on checkout.
        check=ConnectionPool.check_connection,
    )


def raise_connection_error(fn):
    # Raises Dramatiq connection error on psycopg error

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


def unlisten_all(conn):
    # Clear subscriptions, pending notifications and autocommit before the
    # connection goes back to the pool.
    if conn.closed:
        return
    try:
        conn.execute("UNLISTEN *;")
        for _ in conn.notifies(timeout=0):
            pass
        conn.autocommit = False
    except DISCONNECT_ERRORS:
        pass


@contextmanager
def transaction(conn_or_pool, listen=None):
    # Manage the connection, transaction and cursor from a connection pool.
    new_conn = hasattr(conn_or_pool, "getconn")
    with ExitStack() as defer:
        if new_conn:
            conn = getconn(conn_or_pool)
            defer.callback(conn_or_pool.putconn, conn)
        else:
            conn = conn_or_pool

        if listen:
            # This is for NOTIFY consistency, according to psycopg doc.
            conn.autocommit = True
            maybe_transaction_context = nullcontext()
        else:
            maybe_transaction_context = conn.transaction()

        with maybe_transaction_context:
            with conn.cursor() as curs:
                if listen:
                    defer.callback(unlisten_all, conn)
                    curs.execute(
                        sql.SQL("LISTEN {};").format(sql.Identifier(listen))
                    )
                yield curs


def wait_for_notifies(conn, timeout=1):
    # Wait up to timeout for the first notification and return it, plus any
    # sibling delivered in the same packet.
    notifies = list(conn.notifies(timeout=timeout, stop_after=1))
    if notifies:
        logger.debug("Received %d Postgres notifies.", len(notifies))
    return notifies


class QueryManager:
    # Queries are exposed as instance attributes built at runtime.
    if TYPE_CHECKING:

        def __getattr__(self, name: str) -> str: ...

    def __init__(self, queries, schema="dramatiq", prefix=""):
        self.queries = queries
        self.schema = schema
        self.prefix = prefix
        self.build_queries(schema, prefix)

    def build_queries(self, schema=None, prefix=None):
        if not (schema or prefix):
            return
        schema = schema or "dramatiq"
        prefix = prefix or ""

        for name, query in self.queries.items():
            setattr(
                self,
                name,
                query.format(
                    schema=quote_ident(schema),
                    tablename=quote_ident(prefix + "queue"),
                    workertable=quote_ident(prefix + "worker"),
                    resulttable=quote_ident(prefix + "result"),
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
