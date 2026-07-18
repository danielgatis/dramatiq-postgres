#
#       R E S U L T S
#
# Implements a result backend using Postgres. See
# https://dramatiq.io/cookbook.html#results.
#
# Results live in their own table, apart from the queue, so that acknowledged
# messages can be deleted from the hot queue table immediately.
#

import logging
from textwrap import dedent

from dramatiq.results import ResultBackend, ResultMissing, ResultTimeout
from psycopg.types.json import Json

from .utils import (
    QueryManager,
    make_pool,
    retry_pg,
    tidy4json,
    transaction,
    wait_for_notifies,
)

logger = logging.getLogger(__name__)


class PostgresBackend(ResultBackend):
    def __init__(self, *, url=None, pool=None, schema=None, prefix=None, **kw):
        super().__init__(**kw)

        if url:
            self.pool = make_pool(url)
        else:
            # Receive a pool object to have an I/O less __init__.
            self.pool = pool

        QUERIES.build_queries(schema, prefix)

    def build_message_key(self, message):
        # Just use message_id, it's UNIQUE in table.
        return str(message.message_id)

    @retry_pg
    def get_result(self, message, *, block=False, timeout=None):
        key = self.build_message_key(message)

        # Ensure a timeout is set.
        timeout = (timeout or 300_000) // 1000
        channel = f"dramatiq.{key}.results"
        with transaction(self.pool, listen=channel) as curs:
            # First, search result in table.
            curs.execute(QUERIES.GET, (key,))
            if curs.rowcount:
                (result,) = curs.fetchone()
                return self.unwrap_result(result)
            elif not block:
                raise ResultMissing(message)

            # From here, we are in blocking mode.
            logger.debug("Waiting for result of %s.", key)
            notifies = wait_for_notifies(curs.connection, timeout=timeout)

        if not notifies:
            raise ResultTimeout(message)
        (notify,) = notifies
        if not notify.payload:
            # Result was too large for a NOTIFY payload. Fetch it from the
            # table instead.
            with transaction(self.pool) as curs:
                curs.execute(QUERIES.GET, (key,))
                if not curs.rowcount:
                    raise ResultMissing(message)
                (result,) = curs.fetchone()
            return self.unwrap_result(result)
        # Don't query database, use NOTIFY payload.
        decoded = self.encoder.decode(notify.payload.encode("utf-8"))

        return self.unwrap_result(decoded)

    @retry_pg
    def _store(self, key, result, ttl):
        with transaction(self.pool) as curs:
            logger.debug("Storing result for %s.", key)
            curs.execute(
                QUERIES.STORE,
                (
                    key,
                    Json(tidy4json(result)),
                    f"{ttl} ms",
                ),
            )
            if 0 == curs.rowcount:
                raise Exception(f"Can't store result of message {key}.")


QUERIES = QueryManager(
    dict(
        GET=dedent("""\
    SELECT "result"
        FROM {schema}.{resulttable}
        WHERE message_id = %s
            AND (expires_at IS NULL OR expires_at > NOW());
    """),
        STORE=dedent("""\
    WITH stored AS (
        INSERT INTO {schema}.{resulttable} (message_id, "result", expires_at)
            VALUES (%s, %s, NOW() + %s::interval)
        ON CONFLICT (message_id)
        DO UPDATE SET "result" = EXCLUDED."result",
                      expires_at = EXCLUDED.expires_at
        RETURNING message_id, "result"
    )
    SELECT
        pg_notify('dramatiq.' || message_id || '.results',
            CASE WHEN octet_length("result"::text) >= 7900
            THEN ''
            ELSE "result"::text
            END
        )
    FROM stored;
    """),
    )
)
