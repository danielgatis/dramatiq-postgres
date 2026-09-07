import os

import dramatiq
import pytest


@pytest.fixture(scope="module")
def django_project():
    """A minimal Django project wired to dramatiq-postgres.

    Uses a dedicated Postgres schema so the app's migration actually
    exercises initialisation instead of no-oping on the schema created
    by conftest.
    """
    import django
    from django.conf import settings

    # django.setup() replaces the global broker (apps.ready calls
    # set_broker), remember the current one to restore it on teardown.
    previous_broker = dramatiq.get_broker()

    settings.configure(
        INSTALLED_APPS=["dramatiq_postgres.django"],
        DATABASES={
            "default": {
                "ENGINE": "django.db.backends.postgresql",
                "NAME": os.environ["PGDATABASE"],
                "USER": os.environ["PGUSER"],
                "PASSWORD": os.environ["PGPASSWORD"],
                "HOST": os.environ["PGHOST"],
                "PORT": os.environ["PGPORT"],
            }
        },
        DRAMATIQ_BROKER={
            "OPTIONS": {"schema": "dramatiq_django", "listen": False}
        },
        USE_TZ=True,
    )
    django.setup()
    yield settings
    dramatiq.get_broker().close()
    dramatiq.set_broker(previous_broker)
    # PostgresBroker(schema=None) keeps the last built queries, so reset
    # the globals polluted by the custom schema before other modules run.
    from dramatiq_postgres import broker, results

    broker.QUERIES.build_queries("dramatiq", "")
    results.QUERIES.build_queries("dramatiq", "")


def test_migrate_and_enqueue(django_project):
    from django.apps import apps
    from django.core.management import call_command
    from django.db import connection

    call_command("migrate", verbosity=0)

    with connection.cursor() as curs:
        curs.execute("SELECT to_regclass('dramatiq_django.queue');")
        assert curs.fetchone()[0] is not None

    broker = apps.get_app_config("dramatiq_postgres").broker
    assert broker is dramatiq.get_broker()

    @dramatiq.actor(queue_name="django_q")
    def noop():
        pass

    noop.send()

    with connection.cursor() as curs:
        curs.execute(
            "SELECT count(*) FROM dramatiq_django.queue"
            " WHERE queue_name = 'django_q' AND state = 'queued';"
        )
        assert curs.fetchone()[0] == 1


def test_migrate_is_idempotent(django_project):
    from django.core.management import call_command

    call_command("migrate", verbosity=0)
    call_command("migrate", verbosity=0)


def test_db_connections_middleware_is_added(django_project):
    from django.apps import apps

    from dramatiq_postgres.django.middleware import DbConnectionsMiddleware

    broker = apps.get_app_config("dramatiq_postgres").broker
    assert any(isinstance(m, DbConnectionsMiddleware) for m in broker.middleware)


def test_db_connections_middleware_closes_connections(django_project, mocker):
    from dramatiq_postgres.django.middleware import DbConnectionsMiddleware

    close_old = mocker.patch("django.db.close_old_connections")
    close_all = mocker.patch("django.db.connections.close_all")

    mw = DbConnectionsMiddleware()

    mw.before_process_message(None, None)
    mw.after_process_message(None, None)
    assert close_old.call_count == 2
    assert close_all.call_count == 0

    mw.before_worker_shutdown(None)
    mw.before_worker_thread_shutdown(None, None)
    mw.before_consumer_thread_shutdown(None, None)
    assert close_all.call_count == 3
