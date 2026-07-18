import pytest

from dramatiq_postgres.utils import make_pool


class TestMakePool:
    @pytest.fixture
    def cp(self, mocker):
        cp = mocker.patch("dramatiq_postgres.utils.ConnectionPool")
        yield cp
        cp.reset_mock()

    def test_empty(self, cp):
        make_pool("")

        call = cp.call_args
        assert 0 == call.kwargs["min_size"]
        assert 16 == call.kwargs["max_size"]

    def test_keyword(self, cp):
        make_pool("dbname=toto")

        call = cp.call_args
        assert "dbname=toto" in call.args[0]
        assert 16 == call.kwargs["max_size"]

    def test_url_param(self, cp):
        make_pool("postgresql:///?minconn=4")

        call = cp.call_args
        assert "minconn" not in call.args[0]
        assert "minconn" not in call.kwargs["kwargs"]
        assert 4 == call.kwargs["min_size"]

    def test_min_max(self, cp):
        make_pool("postgresql://host/?minconn=4&maxconn=10")

        call = cp.call_args
        assert "maxconn" not in call.args[0]
        assert "maxconn" not in call.kwargs["kwargs"]
        assert 4 == call.kwargs["min_size"]
        assert 10 == call.kwargs["max_size"]

    def test_dict(self, cp):
        make_pool({"host": "hostname", "minconn": 10})

        call = cp.call_args
        assert call.kwargs["kwargs"]["host"] == "hostname"
        assert 10 == call.kwargs["min_size"]


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
