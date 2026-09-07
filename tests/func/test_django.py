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
        INSTALLED_APPS=[
            # contrib.admin and its dependencies, so the app's admin module
            # can be imported and exercised.
            "django.contrib.contenttypes",
            "django.contrib.auth",
            "django.contrib.messages",
            "django.contrib.admin",
            "dramatiq_postgres.django",
        ],
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
        SECRET_KEY="dramatiq-postgres-tests",
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
    assert any(
        isinstance(m, DbConnectionsMiddleware) for m in broker.middleware
    )


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


def test_models_map_the_broker_tables(django_project):
    from django.core.management import call_command

    from dramatiq_postgres.django.models import Job, Result, Worker

    call_command("migrate", verbosity=0)

    # The fixture configures a custom schema, so this also covers the
    # retargeting done by the AppConfig.
    assert Job._meta.db_table == '"dramatiq_django"."queue"'
    assert Worker._meta.db_table == '"dramatiq_django"."worker"'
    assert Result._meta.db_table == '"dramatiq_django"."result"'

    # Unmanaged: the tables come from schema.sql, never from Django.
    assert not Job._meta.managed

    import dramatiq

    @dramatiq.actor(queue_name="admin_q")
    def probe():
        pass

    probe.send()

    msg = Job.objects.get(queue_name="admin_q")
    assert msg.state == Job.QUEUED
    assert msg.actor_name == "probe"


@pytest.fixture
def admins(django_project):
    from django.contrib import admin as dj_admin

    from dramatiq_postgres.django.admin import (
        JobAdmin,
        ResultAdmin,
        WorkerAdmin,
    )
    from dramatiq_postgres.django.models import Job, Result, Worker

    return [
        JobAdmin(Job, dj_admin.site),
        WorkerAdmin(Worker, dj_admin.site),
        ResultAdmin(Result, dj_admin.site),
    ]


def test_jobs_and_workers_cannot_be_edited(admins):
    """The tables are the broker's live state; hand edits corrupt the queue."""
    for site in admins:
        assert not site.has_add_permission(None)
        assert not site.has_change_permission(None)
        # Deletion goes through the actions, which guard on state.
        assert not site.has_delete_permission(None)


def test_only_job_admin_exposes_actions(admins):
    from dramatiq_postgres.django.admin import JobAdmin

    for site in admins:
        if isinstance(site, JobAdmin):
            assert set(site.actions) == {"action_retry", "action_discard"}
        else:
            assert not site.actions


def test_job_columns_come_from_the_payload(django_project):
    from django.contrib import admin as dj_admin

    from dramatiq_postgres.django.admin import JobAdmin
    from dramatiq_postgres.django.models import Job

    site = JobAdmin(Job, dj_admin.site)
    job = Job(
        queue_name="default",
        state=Job.QUEUED,
        message={
            "actor_name": "myapp.tasks.send_email",
            "args": ["someone@example.com"],
            "kwargs": {"urgent": True},
            "options": {"retries": 2},
        },
    )

    # The column shows the short name with the dotted path underneath.
    actor = site.actor_col(job)
    assert "send_email" in actor
    assert "myapp.tasks.send_email" in actor
    assert "2" in site.retries_col(job)

    params = site.params_col(job)
    assert "someone@example.com" in params
    assert "urgent" in params
    assert "queued" in site.state_badge(job)


def test_job_columns_tolerate_a_missing_payload(django_project):
    from django.contrib import admin as dj_admin

    from dramatiq_postgres.django.admin import JobAdmin
    from dramatiq_postgres.django.models import Job

    site = JobAdmin(Job, dj_admin.site)
    job = Job(queue_name="default", state=None, message=None)

    assert site.retries_col(job) == "\u2014"
    assert site.traceback_col(job) == "\u2014"
    assert site.worker_col(job) == "\u2014"
    assert "no parameters" in site.params_col(job)


def test_worker_liveness_uses_the_heartbeat_ttl(django_project):
    import datetime

    from django.contrib import admin as dj_admin
    from django.utils import timezone

    from dramatiq_postgres.django.admin import WorkerAdmin
    from dramatiq_postgres.django.models import Worker

    site = WorkerAdmin(Worker, dj_admin.site)
    now = timezone.now()

    assert "alive" in site.status_badge(Worker(heartbeat_at=now))
    stale = now - datetime.timedelta(seconds=3600)
    assert "dead" in site.status_badge(Worker(heartbeat_at=stale))


def test_retry_skips_running_jobs(django_project):
    """A consumed job is held by a worker: requeuing it double-executes."""
    from django.contrib import admin as dj_admin
    from django.contrib.messages.storage.fallback import FallbackStorage
    from django.test import RequestFactory

    from dramatiq_postgres.django.admin import JobAdmin
    from dramatiq_postgres.django.models import Job

    request = RequestFactory().post("/")
    request.session = {}
    request._messages = FallbackStorage(request)

    site = JobAdmin(Job, dj_admin.site)
    running = Job.objects.filter(state=Job.CONSUMED)
    before = running.count()

    site.action_retry(request, running)
    site.action_discard(request, Job.objects.filter(state=Job.CONSUMED))

    assert Job.objects.filter(state=Job.CONSUMED).count() == before
