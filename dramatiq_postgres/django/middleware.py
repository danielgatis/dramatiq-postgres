"""Django-aware middleware for dramatiq workers."""

from django import db
from dramatiq.middleware import Middleware

__all__ = ["DbConnectionsMiddleware"]


class DbConnectionsMiddleware(Middleware):
    """Keeps Django's database connections healthy inside workers.

    Django caches one connection per thread and relies on the request cycle to
    clean it up. Dramatiq workers are long-lived threads with no request cycle,
    so without this a connection is opened once and held forever: it goes stale
    on an idle timeout or a failover, and the next task fails with
    ``OperationalError``. Connections are also left dangling on shutdown.

    ``close_old_connections`` is a no-op under ``CONN_MAX_AGE = 0`` or a
    psycopg pool, which already handle staleness; the shutdown hooks matter in
    every configuration.

    Added automatically by :class:`~dramatiq_postgres.django.apps.DramatiqPostgresConfig`
    unless it is already present in the configured middleware.
    """

    def _close_old_connections(self, *args, **kwargs):
        db.close_old_connections()

    before_process_message = _close_old_connections
    after_process_message = _close_old_connections

    def _close_connections(self, *args, **kwargs):
        db.connections.close_all()

    before_consumer_thread_shutdown = _close_connections
    before_worker_thread_shutdown = _close_connections
    before_worker_shutdown = _close_connections
