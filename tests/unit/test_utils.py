import pytest

from dramatiq_postgres.utils import make_pool


class TestMakePool:
    @pytest.fixture
    def tp(self, mocker):
        tp = mocker.patch("dramatiq_postgres.utils.ThreadedConnectionPool")
        yield tp
        tp.reset_mock()

    def test_empty(self, tp):
        pool = make_pool("")

        assert 16 == pool.minconn

    def test_keyword(self, tp):
        pool = make_pool("dbname=toto")

        call = tp.mock_calls[0]
        assert "dbname=toto" in call[1][2]
        assert 16 == pool.minconn

    def test_url_param(self, tp):
        pool = make_pool("postgresql:///?minconn=4")

        call = tp.mock_calls[0]
        assert "minconn" not in call[1][2]
        assert 4 == pool.minconn

    def test_min_max(self, tp):
        pool = make_pool("postgresql://host/?minconn=4&maxconn=10")

        call = tp.mock_calls[0]
        assert "maxconn" not in call[1][2]
        assert 4 == pool.minconn

    def test_dict(self, tp):
        pool = make_pool({"host": "hostname", "minconn": 10})

        call = tp.mock_calls[0]
        assert "host" in call.kwargs
        assert call.kwargs["host"] == "hostname"
        assert 10 == pool.minconn


def test_quote_ident():
    from dramatiq_postgres.utils import quote_ident

    assert '"table"' == quote_ident("table")
    assert '"with space"' == quote_ident("with space")
    assert '"with""quote"' == quote_ident('with"quote')


def test_query_manager_schema_only():
    from dramatiq_postgres.utils import QueryManager

    qm = QueryManager({"get": "SELECT 1 FROM {schema}.{tablename};"})
    qm.build_queries(schema="custom")
    assert qm.get == 'SELECT 1 FROM "custom"."queue";'

    qm.build_queries(prefix="pfx_")
    assert qm.get == 'SELECT 1 FROM "dramatiq"."pfx_queue";'
