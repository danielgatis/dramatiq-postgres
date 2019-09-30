# For now, just run unit test along func tests.


def test_make_pool(mocker):
    tp = mocker.patch('dramatiq_pg.utils.ThreadedConnectionPool')
    from dramatiq_pg.utils import make_pool

    pool = make_pool("")
    tp.assert_called_with(0, 16, "")
    assert 16 == pool.minconn
    tp.reset_mock()

    pool = make_pool("")
    tp.assert_called_with(0, 16, "")
    assert 16 == pool.minconn
    tp.reset_mock()

    pool = make_pool("dbname=toto")
    tp.assert_called_with(0, 16, "dbname=toto")
    assert 16 == pool.minconn
    tp.reset_mock()

    pool = make_pool("postgresql:///?minconn=4")
    tp.assert_called_with(0, 16, "postgresql:///")
    assert 4 == pool.minconn
    tp.reset_mock()

    pool = make_pool("postgresql://host/?minconn=4&maxconn=10")
    tp.assert_called_with(0, 10, "postgresql://host/")
    assert 4 == pool.minconn
    tp.reset_mock()
