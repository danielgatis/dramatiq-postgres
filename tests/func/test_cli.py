import re

from sh import dramatiq_postgres

FAKE_WORKER = "00000000-0000-0000-0000-000000000001"


def test_stats():
    out = dramatiq_postgres("stats", _truncate_exc=False)
    assert "queued: " in out


def test_purge():
    out = dramatiq_postgres("purge", "--maxage", "1 second", _err_to_out=True)
    assert "Deleted" in out


def test_recover(pgconn):
    PREFILL = 8
    # Fake messages consumed by a live worker, on a queue no worker consumes,
    # so neither the workers nor the reaper touch them before the CLI does.
    with pgconn() as curs:
        curs.execute(
            """
            INSERT INTO dramatiq.worker (worker_id, heartbeat_at)
            VALUES (%s, NOW())
            ON CONFLICT (worker_id) DO UPDATE SET heartbeat_at = NOW();
            """,
            (FAKE_WORKER,),
        )
        for i in range(PREFILL):
            curs.execute(
                """
                INSERT INTO dramatiq.queue
                    (message_id, queue_name, state, message,
                     worker_id, consumed_at)
                VALUES (gen_random_uuid(), 'clitest', 'consumed',
                        '{}'::jsonb, %s, NOW());
                """,
                (FAKE_WORKER,),
            )

    out = dramatiq_postgres(
        "recover", "--minage", "10 microsecond", _err_to_out=True
    )
    assert re.search(r"(?:\d{2,}|[^0]) messages", str(out))

    with pgconn() as curs:
        curs.execute(
            "DELETE FROM dramatiq.queue WHERE queue_name = 'clitest';"
        )
        curs.execute(
            "DELETE FROM dramatiq.worker WHERE worker_id = %s;", (FAKE_WORKER,)
        )


def test_flush():
    out = dramatiq_postgres("flush", _err_to_out=True)
    assert "Flushed" in out


def test_init(pgconn):
    with pgconn() as curs:
        curs.execute("DROP SCHEMA IF EXISTS inittest CASCADE;")

    out = dramatiq_postgres(
        "--schemaname", "inittest", "init", _err_to_out=True
    )
    assert "Initialized" in out

    with pgconn() as curs:
        curs.execute(
            "SELECT to_regclass('inittest.queue'),"
            " to_regclass('inittest.worker'),"
            " to_regclass('inittest.result');"
        )
        assert all(curs.fetchone())

    # Idempotent: second run is a no-op.
    out = dramatiq_postgres(
        "--schemaname", "inittest", "init", _err_to_out=True
    )
    assert "already initialized" in out

    with pgconn() as curs:
        curs.execute("DROP SCHEMA inittest CASCADE;")
