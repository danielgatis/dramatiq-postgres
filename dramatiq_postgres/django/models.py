"""Unmanaged models over the broker tables.

The tables come from this app's migration, which runs schema.sql — Django
never creates or alters them.

``db_table`` carries the stock name and the AppConfig rewrites it in
``ready()`` for a custom ``schema``/``prefix``: it cannot be resolved in
``Meta``, which is evaluated before settings are guaranteed to be configured.
"""

from django.db import models
from django.utils.translation import gettext_lazy as _


def table_name(name, schema=None, prefix=None):
    """Quoted "schema"."prefixname", matching what the broker builds."""
    return f'"{schema or "dramatiq"}"."{prefix or ""}{name}"'


class Job(models.Model):
    """A message in the queue.

    Only pending work and failures live here: a message is deleted as soon as
    it is acknowledged, so completed tasks do not accumulate.
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
        verbose_name = _("job")
        verbose_name_plural = _("jobs")

    def __str__(self):
        return f"{self.actor_name or '?'} ({self.message_id})"

    def _opt(self, key, default=None):
        if not isinstance(self.message, dict):
            return default
        return (self.message.get("options") or {}).get(key, default)

    @property
    def actor_name(self):
        if not isinstance(self.message, dict):
            return None
        return self.message.get("actor_name")

    @property
    def args(self):
        return (self.message or {}).get("args") if self.message else None

    @property
    def kwargs(self):
        return (self.message or {}).get("kwargs") if self.message else None

    @property
    def retries(self):
        return self._opt("retries") or 0

    @property
    def traceback(self):
        return self._opt("traceback")

    @property
    def is_running(self):
        return self.state == self.CONSUMED


class Result(models.Model):
    """The return value of an actor declared with ``store_results=True``.

    Outlives the job: the queue row is deleted on ack, this one stays until
    ``expires_at``.
    """

    # dramatiq stores the raw value; only an exception gets an envelope,
    # marked by this canary (dramatiq/results/result.py).
    CANARY = "dramatiq.results.Result"

    message_id = models.UUIDField(primary_key=True)
    result = models.JSONField(null=True)
    expires_at = models.DateTimeField(null=True)

    class Meta:
        managed = False
        db_table = table_name("result")
        verbose_name = _("result")
        verbose_name_plural = _("results")

    def __str__(self):
        return str(self.message_id)

    @property
    def is_failure(self):
        return (
            isinstance(self.result, dict)
            and self.result.get("__t") == self.CANARY
        )

    @property
    def error(self):
        """(type, message) when the actor raised."""
        if not self.is_failure:
            return None
        exn = self.result.get("exn") or {}
        return exn.get("type"), exn.get("msg")

    @property
    def payload(self):
        """The actor's return value, or None when it failed."""
        return None if self.is_failure else self.result


class Worker(models.Model):
    """A worker process, kept alive by its heartbeat."""

    worker_id = models.UUIDField(primary_key=True)
    heartbeat_at = models.DateTimeField()

    class Meta:
        managed = False
        db_table = table_name("worker")
        verbose_name = _("worker")
        verbose_name_plural = _("workers")

    def __str__(self):
        return str(self.worker_id)
