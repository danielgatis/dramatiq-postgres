import os

from testcontainers.postgres import PostgresContainer

# Global container started before test collection
_postgres_container = None


def _clean_sql(sql):
    """Remove psql commands and substitute variables."""
    variables = {}

    # Extract \set variables from SQL
    for line in sql.split("\n"):
        if line.strip().startswith("\\set "):
            parts = line.strip().split(None, 2)
            if len(parts) >= 3:
                var_name = parts[1]
                var_value = parts[2].strip("'")
                variables[var_name] = var_value

    # Remove lines starting with \ (psql commands)
    lines = [
        line for line in sql.split("\n") if not line.strip().startswith("\\")
    ]
    result = "\n".join(lines)

    # Substitute :"var" with value (psql format)
    for var_name, var_value in variables.items():
        result = result.replace(f':"{var_name}"', var_value)

    return result


def pytest_configure(config):
    """Start PostgreSQL container before test collection."""
    global _postgres_container

    # Speed up crash recovery so func tests don't wait the production
    # heartbeat TTL. Inherited by dramatiq worker subprocesses.
    os.environ.setdefault("DRAMATIQ_PG_HEARTBEAT_INTERVAL", "1")
    os.environ.setdefault("DRAMATIQ_PG_HEARTBEAT_TTL", "3")
    os.environ.setdefault("DRAMATIQ_PG_MAINTENANCE_INTERVAL", "1")

    # Skip container if SKIP_TESTCONTAINERS is set (use local PG)
    if os.environ.get("SKIP_TESTCONTAINERS"):
        return

    _postgres_container = PostgresContainer("postgres:16-alpine")
    _postgres_container.start()

    # Configure environment variables for psycopg2
    os.environ["PGHOST"] = _postgres_container.get_container_host_ip()
    os.environ["PGPORT"] = str(_postgres_container.get_exposed_port(5432))
    os.environ["PGUSER"] = _postgres_container.username
    os.environ["PGPASSWORD"] = _postgres_container.password
    os.environ["PGDATABASE"] = _postgres_container.dbname

    # Initialize dramatiq-postgres and functest schemas
    import psycopg2

    conn = psycopg2.connect("")
    conn.autocommit = True
    with conn.cursor() as cur:
        # Load dramatiq-postgres schema
        schema_path = os.path.join(
            os.path.dirname(__file__), "..", "dramatiq_postgres", "schema.sql"
        )
        with open(schema_path) as f:
            cur.execute(_clean_sql(f.read()))

        # Load functest schema
        functest_schema = os.path.join(
            os.path.dirname(__file__), "func", "schema.sql"
        )
        with open(functest_schema) as f:
            cur.execute(_clean_sql(f.read()))
    conn.close()


def pytest_unconfigure(config):
    """Stop PostgreSQL container after tests."""
    global _postgres_container
    if _postgres_container:
        _postgres_container.stop()
        _postgres_container = None
