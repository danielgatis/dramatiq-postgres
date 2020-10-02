import os.path

from .utils import quote_ident


def process_psql_lines(raw_lines, schema, prefix):
    for line in raw_lines:
        if line.startswith('\\'):
            continue
        yield (
            line
            .replace(':"schema"', quote_ident(schema))
            .replace(':"state"', quote_ident(prefix + "state"))
            .replace(':"queue"', quote_ident(prefix + "queue"))
        )


def generate_init_sql(schema="dramatiq", prefix=""):
    """Returns SQL for schema initialisation

    Interpolate schema and prefix and return a single SQL string for execution
    on a PostgreSQL connection.
    """

    path = os.path.dirname(__file__) + '/schema.sql'
    with open(path) as fo:
        return '\n'.join(process_psql_lines(fo, schema, prefix))
