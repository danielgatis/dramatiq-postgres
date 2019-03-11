# For now, just run unit test along func tests.


def test_make_pool(mocker):
    tp = mocker.patch('dramatiq_pg.broker.ThreadedConnectionPool')
    from dramatiq_pg.broker import make_pool

    make_pool("")
    tp.assert_called_with(0, 16, "")
    tp.reset_mock()

    make_pool("")
    tp.assert_called_with(0, 16, "")
    tp.reset_mock()

    make_pool("dbname=toto")
    tp.assert_called_with(0, 16, "dbname=toto")
    tp.reset_mock()

    make_pool("postgresql:///?minconn=4")
    tp.assert_called_with(4, 16, "postgresql:///")
    tp.reset_mock()

    make_pool("postgresql://host/?minconn=4&maxconn=10")
    tp.assert_called_with(4, 10, "postgresql://host/")
    tp.reset_mock()
