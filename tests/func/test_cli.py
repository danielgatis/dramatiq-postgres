import re

from sh import dramatiq_pg

from example import writer


def test_stats():
    out = dramatiq_pg('stats', _truncate_exc=False)
    assert 'done: ' in out


def test_purge():
    out = dramatiq_pg('purge', '--maxage', '1 second', _err_to_out=True)
    assert 'Deleted' in out


def test_recover(pgconn):
    PREFILL = 8
    for i in range(PREFILL):
        writer.send(message='prefill', index=i)

    # Fake consumption of message by a worker.
    with pgconn() as curs:
        curs.execute("UPDATE dramatiq.queue SET state = 'consumed';")

    out = dramatiq_pg('recover', '--minage', '10 microsecond')
    assert re.search(br'(?:\d{2,}|[^0]) messages', out.stderr)


def test_flush():
    out = dramatiq_pg('flush', _err_to_out=True)
    assert 'Flushed' in out
