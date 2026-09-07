"""Read-only admin for the broker tables.

Everything here is deliberately read-only: these tables are the broker's live
state, and editing a row by hand corrupts the queue (a claimed message whose
``worker_id`` is cleared gets delivered twice, for instance). Use the CLI or
the broker API to act on messages.
"""

import datetime

from django.conf import settings
from django.contrib import admin
from django.utils import timezone
from django.utils.html import format_html

from .models import Message, Result, Worker


class ReadOnlyAdmin(admin.ModelAdmin):
    """Disables every write path, including the bulk-delete action."""

    actions = None

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


@admin.register(Message)
class MessageAdmin(ReadOnlyAdmin):
    list_display = (
        "message_id",
        "actor_display",
        "queue_name",
        "state_display",
        "retries_display",
        "available_at",
        "mtime",
    )
    list_filter = ("state", "queue_name")
    search_fields = ("message_id", "queue_name")
    ordering = ("-mtime",)
    date_hierarchy = "mtime"
    fields = (
        "message_id",
        "queue_name",
        "state",
        "actor_display",
        "args_display",
        "kwargs_display",
        "retries_display",
        "position",
        "available_at",
        "mtime",
        "worker_id",
        "consumed_at",
        "traceback_display",
        "message",
    )
    readonly_fields = fields

    @admin.display(description="actor", ordering="message__actor_name")
    def actor_display(self, obj):
        return obj.actor_name or "—"

    @admin.display(description="state")
    def state_display(self, obj):
        colors = {
            Message.QUEUED: "#1a73e8",
            Message.CONSUMED: "#188038",
            Message.REJECTED: "#d93025",
        }
        color = colors.get(obj.state, "#5f6368")
        return format_html(
            '<b style="color:{}">{}</b>', color, obj.state or "—"
        )

    @admin.display(description="retries")
    def retries_display(self, obj):
        return obj.retries if obj.retries is not None else "—"

    @admin.display(description="args")
    def args_display(self, obj):
        return obj._payload("args") or "—"

    @admin.display(description="kwargs")
    def kwargs_display(self, obj):
        return obj._payload("kwargs") or "—"

    @admin.display(description="traceback")
    def traceback_display(self, obj):
        if not obj.traceback:
            return "—"
        return format_html(
            '<pre style="white-space:pre-wrap;margin:0">{}</pre>',
            obj.traceback,
        )


@admin.register(Worker)
class WorkerAdmin(ReadOnlyAdmin):
    list_display = ("worker_id", "heartbeat_at", "liveness")
    ordering = ("-heartbeat_at",)

    @admin.display(description="status")
    def liveness(self, obj):
        options = getattr(settings, "DRAMATIQ_BROKER", {}).get("OPTIONS", {})
        ttl = options.get("heartbeat_ttl", 60.0)
        deadline = timezone.now() - datetime.timedelta(seconds=ttl)
        if obj.heartbeat_at < deadline:
            return format_html('<b style="color:{}">dead</b>', "#d93025")
        return format_html('<b style="color:{}">alive</b>', "#188038")


@admin.register(Result)
class ResultAdmin(ReadOnlyAdmin):
    list_display = ("message_id", "expires_at")
    search_fields = ("message_id",)
    ordering = ("-expires_at",)
    fields = ("message_id", "result", "expires_at")
    readonly_fields = fields
