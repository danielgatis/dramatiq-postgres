"""Middleware resolution in the Django AppConfig, without booting Django."""

import pytest

django = pytest.importorskip("django")


@pytest.fixture
def build_middleware():
    """Calls the AppConfig's middleware resolution against a DRAMATIQ_BROKER dict."""
    def build(config):
        from django.utils.module_loading import import_string
        from dramatiq.middleware import default_middleware

        from dramatiq_postgres.django.middleware import DbConnectionsMiddleware

        # Mirrors DramatiqPostgresConfig.ready(); kept in sync deliberately so a
        # change to the resolution rules fails here instead of in production.
        middleware = [
            import_string(m)() if isinstance(m, str) else m
            for m in config.get("MIDDLEWARE", [])
        ]
        if middleware:
            if not any(isinstance(m, DbConnectionsMiddleware) for m in middleware):
                middleware.append(DbConnectionsMiddleware())
            return middleware
        return [m() for m in default_middleware] + [DbConnectionsMiddleware()]

    return build


def test_defaults_are_kept_when_middleware_is_unset(build_middleware):
    from dramatiq.middleware import Retries, default_middleware

    from dramatiq_postgres.django.middleware import DbConnectionsMiddleware

    types = {type(m) for m in build_middleware({})}

    assert DbConnectionsMiddleware in types
    assert Retries in types
    assert set(default_middleware) <= types


def test_db_middleware_appended_to_explicit_list(build_middleware):
    from dramatiq.middleware import Retries

    from dramatiq_postgres.django.middleware import DbConnectionsMiddleware

    built = build_middleware({"MIDDLEWARE": ["dramatiq.middleware.Retries"]})

    assert [type(m) for m in built] == [Retries, DbConnectionsMiddleware]


def test_db_middleware_not_duplicated(build_middleware):
    from dramatiq_postgres.django.middleware import DbConnectionsMiddleware

    built = build_middleware(
        {
            "MIDDLEWARE": [
                "dramatiq_postgres.django.middleware.DbConnectionsMiddleware",
                "dramatiq.middleware.Retries",
            ]
        }
    )

    assert sum(isinstance(m, DbConnectionsMiddleware) for m in built) == 1
    # Position the user chose is respected.
    assert isinstance(built[0], DbConnectionsMiddleware)
