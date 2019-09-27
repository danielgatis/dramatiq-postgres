import logging
import select
from contextlib import contextmanager
from urllib.parse import (
    parse_qsl,
    urlencode,
    urlparse,
)

from psycopg2 import OperationalError
from psycopg2.extensions import (
    ISOLATION_LEVEL_AUTOCOMMIT,
    quote_ident,
)
from psycopg2.pool import ThreadedConnectionPool
import tenacity


logger = logging.getLogger(__name__)


def make_pool(url):
    parts = urlparse(url)
    qs = dict(parse_qsl(parts.query))
    minconn = int(qs.pop('minconn', '0'))
    maxconn = int(qs.pop('maxconn', '16'))
    parts = parts._replace(query=urlencode(qs))
    connstring = parts.geturl()
    if ":/?" in connstring or connstring.endswith(':/'):
        # geturl replaces :/// with :/. libpq does not accept that.
        connstring = connstring.replace(':/', ':///')
    return ThreadedConnectionPool(minconn, maxconn, connstring)

@tenacity.retry(
    retry=tenacity.retry_if_exception(OperationalError),
    reraise=True,
    wait=tenacity.wait_random_exponential(multiplier=1, max=30),
    stop=tenacity.stop_after_attempt(7),
    before_sleep=tenacity.before_sleep_log(logger, logging.INFO),
)
def getconn(pool):
    return pool.getconn()


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
        channel = quote_ident(listen, conn)

    try:
        with conn:  # Wraps in a transaction.
            with conn.cursor() as curs:
                if listen:
                    curs.execute(f"LISTEN {channel};")
                yield curs
    finally:
        if new_conn:
            conn_or_pool.putconn(conn)


def wait_for_notifies(conn, timeout=1):
    rlist, *_ = select.select([conn], [], [], timeout)
    conn.poll()
    notifies = conn.notifies[:]
    if notifies:
        logger.debug("Received %d Postgres notifies.", len(conn.notifies))
        conn.notifies[:] = []
    return notifies
