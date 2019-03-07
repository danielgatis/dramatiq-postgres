from sh import dramatiq_pg


def test_stats():
    out = dramatiq_pg('stats')
    assert 'done: ' in out


def test_purge():
    out = dramatiq_pg('purge', '--maxage', '1 second', _err_to_out=True)
    assert 'Deleted' in out
