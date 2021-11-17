def test_make_pool(mocker):
    tp = mocker.patch('dramatiq_pg.utils.ThreadedConnectionPool')
    from dramatiq_pg.utils import make_pool

    pool = make_pool("")
    assert 16 == pool.minconn
    tp.reset_mock()

    pool = make_pool("dbname=toto")
    call = tp.mock_calls[0]
    assert "dbname=toto" in call[1][2]
    assert 16 == pool.minconn
    tp.reset_mock()

    pool = make_pool("postgresql:///?minconn=4")
    call = tp.mock_calls[0]
    assert "minconn" not in call[1][2]
    assert 4 == pool.minconn
    tp.reset_mock()

    pool = make_pool("postgresql://host/?minconn=4&maxconn=10")
    assert "maxconn" not in call[1][2]
    assert 4 == pool.minconn
    tp.reset_mock()


def test_quote_ident():
    from dramatiq_pg.utils import quote_ident

    assert '"table"' == quote_ident("table")
    assert '"with space"' == quote_ident("with space")
    assert '"with""quote"' == quote_ident("with\"quote")
