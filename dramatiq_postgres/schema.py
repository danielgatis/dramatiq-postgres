import os.path

from .utils import quote_ident


def process_psql_lines(raw_lines, schema, prefix):
    schema = quote_ident(schema)
    tablename = quote_ident(prefix + "queue")
    statename = quote_ident(prefix + "state")
    workername = quote_ident(prefix + "worker")
    resultname = quote_ident(prefix + "result")

    for line in raw_lines:
        if line.startswith("\\"):
            continue
        yield (
            line.replace(':"schema"', schema)
            .replace(':"state"', statename)
            .replace(':"queue"', tablename)
            .replace(':"worker"', workername)
            .replace(':"result"', resultname)
        )


def generate_init_sql(schema="dramatiq", prefix=""):
    """Returns SQL for schema initialisation

    Interpolate schema and prefix and return a single SQL string for execution
    on a PostgreSQL connection.
    """

    path = os.path.dirname(__file__) + "/schema.sql"
    with open(path) as fo:
        return "\n".join(process_psql_lines(fo, schema, prefix))


def schema_exists(curs, schema="dramatiq", prefix=""):
    """Returns whether the queue table is installed."""

    curs.execute(
        "SELECT to_regclass(%s);",
        (f"{quote_ident(schema)}.{quote_ident(prefix + 'queue')}",),
    )
    (queue_reg,) = curs.fetchone()
    return queue_reg is not None


def init(curs, schema="dramatiq", prefix=""):
    """Initialize the schema if missing.

    Idempotent and concurrency-safe: serialized on an advisory lock, held
    until the surrounding transaction ends. Returns True when the schema
    was created, False when it was already in place.
    """

    curs.execute(
        "SELECT pg_advisory_xact_lock("
        "hashtext(%s), hashtext('dramatiq-postgres-ddl'));",
        (f"{schema}.{prefix}queue",),
    )
    if schema_exists(curs, schema, prefix):
        return False
    curs.execute(generate_init_sql(schema, prefix))
    return True
