"""Unmanaged models mapping the broker tables, for read-only introspection.

These exist so the queue can be inspected from the Django admin. They are
``managed = False``: the tables are created by the app's migration, which runs
the same ``schema.sql`` as the CLI, and Django must never alter them.

``db_table`` defaults to the stock ``"dramatiq"."<name>"``; the AppConfig
rewrites it in ``ready()`` when ``DRAMATIQ_BROKER["OPTIONS"]`` sets a custom
``schema`` or ``prefix``. It cannot be resolved here: Django needs the table
name when the class is defined, which is before settings are guaranteed to be
configured.
"""

from django.db import models

__all__ = ["Message", "Worker", "Result"]


def table_name(name, schema=None, prefix=None):
    """Quoted "schema"."prefixname", matching what the broker builds."""
    return f'"{schema or "dramatiq"}"."{prefix or ""}{name}"'


class Message(models.Model):
    """A message in the queue table.

    Only pending work and failures are visible here: a message is deleted as
    soon as it is acknowledged, so successfully processed tasks do not
    accumulate. Rejected messages are kept until ``purge_maxage``.
    """

    QUEUED = "queued"
    CONSUMED = "consumed"
    REJECTED = "rejected"

    message_id = models.UUIDField(primary_key=True)
    queue_name = models.TextField()
    state = models.TextField(null=True)
    mtime = models.DateTimeField(null=True)
    message = models.JSONField(null=True)
    position = models.BigIntegerField()
    available_at = models.DateTimeField()
    worker_id = models.UUIDField(null=True)
    consumed_at = models.DateTimeField(null=True)

    class Meta:
        managed = False
        db_table = table_name("queue")
        verbose_name = "message"
        verbose_name_plural = "messages"

    def __str__(self):
        return f"{self.actor_name or '?'} ({self.message_id})"

    def _payload(self, key, default=None):
        if not isinstance(self.message, dict):
            return default
        return self.message.get(key, default)

    @property
    def actor_name(self):
        return self._payload("actor_name")

    @property
    def retries(self):
        return (self._payload("options") or {}).get("retries")

    @property
    def traceback(self):
        return (self._payload("options") or {}).get("traceback")


class Worker(models.Model):
    """A live worker process, identified by its heartbeat.

    A row whose ``heartbeat_at`` is older than the broker's ``heartbeat_ttl``
    is a dead worker; its consumed messages are requeued by maintenance.
    """

    worker_id = models.UUIDField(primary_key=True)
    heartbeat_at = models.DateTimeField()

    class Meta:
        managed = False
        db_table = table_name("worker")
        verbose_name = "worker"
        verbose_name_plural = "workers"

    def __str__(self):
        return str(self.worker_id)


class Result(models.Model):
    """A stored actor result.

    Only populated for actors declared with ``store_results=True``.
    """

    message_id = models.UUIDField(primary_key=True)
    result = models.JSONField(null=True)
    expires_at = models.DateTimeField(null=True)

    class Meta:
        managed = False
        db_table = table_name("result")
        verbose_name = "result"
        verbose_name_plural = "results"

    def __str__(self):
        return str(self.message_id)
