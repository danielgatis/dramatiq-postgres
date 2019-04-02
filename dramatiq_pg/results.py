#
#       R E S U L T S
#
# Implements a result backend using Postgres. See
# https://dramatiq.io/cookbook.html#results.
#

import json
import logging
from textwrap import dedent

from dramatiq.results import ResultBackend, ResultMissing, ResultTimeout
from psycopg2.extras import Json

from .utils import make_pool, transaction, wait_for_notifies


logger = logging.getLogger(__name__)


class PostgresBackend(ResultBackend):
    def __init__(self, *, url=None, pool=None, **kw):
        super().__init__(**kw)

        if url:
            self.pool = make_pool(url)
        else:
            # Receive a pool object to have an I/O less __init__.
            self.pool = pool

    def build_message_key(self, message):
        # Just use message_id, it's UNIQUE in table.
        return str(message.message_id)

    def get_result(self, message, *, block=False, timeout=None):
        # Ensure a timeout is set.
        timeout = timeout or 300_000
        channel = f'dramatiq.{message.message_id}.results'
        with transaction(self.pool, listen=channel) as curs:
            # First, search result in table.
            curs.execute(dedent("""\
            SELECT result
              FROM dramatiq.queue
             WHERE message_id = %s;
            """), (message.message_id,))
            result, = curs.fetchone()

            if result is not None:
                return result
            elif not block:
                raise ResultMissing(message)

            # From here, we are in blocking mode.
            logger.debug("Waiting for result of %s.", message.message_id)
            notifies = wait_for_notifies(curs.connection, timeout=timeout)

        if not notifies:
            raise ResultTimeout(message)
        notify, = notifies
        # Don't query database, use NOTIFY payload.
        return json.loads(notify.payload)

    def _store(self, key, result, ttl):
        with transaction(self.pool) as curs:
            logger.debug("Storing result for %s.", key)
            curs.execute(dedent("""\
            WITH stored AS (
                UPDATE dramatiq.queue
                   SET mtime = (NOW() AT TIME ZONE 'UTC'),
                       result = %s,
                       result_ttl = (NOW() AT TIME ZONE 'UTC') + interval %s
                 WHERE message_id = %s
                RETURNING queue_name, message_id, result
            )
            SELECT
             pg_notify('dramatiq.' || message_id || '.results', result::text)
            FROM stored;
            """), (Json(result), f"{ttl} ms", key))
            if 0 == curs.rowcount:
                raise Exception(f"Can't store result of message {key}.")
