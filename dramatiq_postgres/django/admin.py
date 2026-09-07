"""Job dashboard in the Django admin.

Actions only touch idle jobs (``queued``/``rejected``). A ``consumed`` job is
held by a worker right now — requeuing or deleting it would cause double
execution.
"""

import datetime
import json

import dramatiq
from django.conf import settings
from django.contrib import admin, messages
from django.db.models import Count
from django.urls import reverse
from django.utils import timezone
from django.utils.html import format_html
from django.utils.safestring import mark_safe
from django.utils.translation import gettext_lazy as _
from django.utils.translation import ngettext
from dramatiq.common import current_millis

from .models import Job, Result, Worker

STATE_COLORS = {
    Job.QUEUED: ("#1a73e8", "#e8f0fe"),
    Job.CONSUMED: ("#188038", "#e6f4ea"),
    Job.REJECTED: ("#d93025", "#fce8e6"),
}

STATE_LABELS = {
    Job.QUEUED: _("queued"),
    Job.CONSUMED: _("running"),
    Job.REJECTED: _("failed"),
}

# The Django admin styles any <table> inside the content area (full width, its
# own borders and padding). Everything here is prefixed and uses !important
# where the admin would otherwise win.
SUMMARY_CSS = """
.dq-wrap { margin: 0 0 20px; }
.dq-cards { display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 16px; }
.dq-card { flex: 1; min-width: 130px; border-radius: 8px; padding: 14px 18px; }
.dq-card .n { font-size: 28px; font-weight: 700; line-height: 1.1; }
.dq-card .l { font-size: 12px; opacity: .85; margin-top: 2px; }
table.dq-summary {
  width: 100% !important;
  border-collapse: collapse !important;
  font-size: 13px;
  background: #fff;
  border: 1px solid #e0e0e0 !important;
  border-radius: 6px;
  overflow: hidden;
}
table.dq-summary th,
table.dq-summary td {
  padding: 7px 18px !important;
  border: 0 !important;
  text-align: right;
  white-space: nowrap;
  line-height: 1.4;
}
table.dq-summary th {
  background: #f5f5f5 !important;
  color: #444 !important;
  font-weight: 600;
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: .04em;
}
table.dq-summary th:first-child,
table.dq-summary td:first-child { text-align: left; width: 100%; }
/* Number columns stay snug; the queue column absorbs the slack. */
table.dq-summary th:not(:first-child),
table.dq-summary td:not(:first-child) { width: 1%; }
table.dq-summary tbody tr { border-top: 1px solid #eee !important; }
table.dq-summary td.q { font-family: monospace; color: #222; }
table.dq-summary tfoot tr {
  border-top: 2px solid #ddd !important;
  background: #fafafa !important;
  font-weight: 600;
}
table.dq-summary .zero { color: #bbb; }
"""


def _join(parts):
    """Join already-escaped fragments without escaping them again."""
    return mark_safe("".join(parts))


def _heartbeat_ttl():
    """Seconds without a heartbeat before a worker counts as dead."""
    options = getattr(settings, "DRAMATIQ_BROKER", {}).get("OPTIONS", {})
    return options.get("heartbeat_ttl", 60.0)


def _worker_link(worker_id, short=False):
    """Link to the worker; plain text once its row is gone."""
    if not worker_id:
        return "—"
    label = str(worker_id)[:8] if short else str(worker_id)
    if not Worker.objects.filter(pk=worker_id).exists():
        return format_html(
            '<code title="{}">{}</code>', _("worker no longer exists"), label
        )
    url = reverse(
        "admin:dramatiq_postgres_worker_change", args=[str(worker_id)]
    )
    return format_html('<a href="{}"><code>{}</code></a>', url, label)


def _badge(text, fg, bg):
    return format_html(
        '<span style="background:{};color:{};padding:2px 8px;'
        "border-radius:10px;font-weight:600;font-size:11px;"
        'white-space:nowrap">{}</span>',
        bg,
        fg,
        text,
    )


def _card(label, n, colors):
    fg, bg = colors
    return format_html(
        '<div class="dq-card" style="background:{}">'
        '<div class="n" style="color:{}">{}</div>'
        '<div class="l" style="color:{}">{}</div></div>',
        bg,
        fg,
        n,
        fg,
        label,
    )


class AvailabilityFilter(admin.SimpleListFilter):
    """Jobs scheduled for the future vs. ready to run now."""

    title = _("availability")
    parameter_name = "availability"

    def lookups(self, request, model_admin):
        return (("ready", _("Ready now")), ("scheduled", _("Scheduled")))

    def queryset(self, request, queryset):
        now = timezone.now()
        if self.value() == "ready":
            return queryset.filter(available_at__lte=now)
        if self.value() == "scheduled":
            return queryset.filter(available_at__gt=now)
        return queryset


@admin.register(Job)
class JobAdmin(admin.ModelAdmin):
    list_display = (
        "state_badge",
        "actor_col",
        "queue_name",
        "retries_col",
        "when_col",
        "worker_col",
    )
    list_filter = ("state", "queue_name", AvailabilityFilter)
    search_fields = ("message_id", "queue_name")
    ordering = ("-mtime",)
    list_per_page = 50
    actions = ("action_retry", "action_discard")

    fields = (
        "message_id",
        "state_badge",
        "actor_col",
        "queue_name",
        "params_col",
        "result_col",
        "retries_col",
        "available_at",
        "mtime",
        "consumed_at",
        "worker_link",
        "traceback_col",
    )
    readonly_fields = fields

    def lookup_allowed(self, lookup, value, request=None):
        # Allow ?worker_id=<uuid>, used by the link from the workers page.
        if lookup == "worker_id":
            return True
        return super().lookup_allowed(lookup, value, request)

    # ---- columns -------------------------------------------------------

    @admin.display(description=_("state"), ordering="state")
    def state_badge(self, obj):
        fg, bg = STATE_COLORS.get(obj.state, ("#5f6368", "#f1f3f4"))
        return _badge(STATE_LABELS.get(obj.state, obj.state or "—"), fg, bg)

    @admin.display(description=_("actor"))
    def actor_col(self, obj):
        name = obj.actor_name or "—"
        return format_html(
            '<b>{}</b><br><small style="color:#777">{}</small>',
            name.rsplit(".", 1)[-1],
            name,
        )

    @admin.display(description=_("retries"))
    def retries_col(self, obj):
        if not obj.retries:
            return "—"
        return format_html('<b style="color:#b06000">{}</b>', obj.retries)

    @admin.display(description=_("when"), ordering="mtime")
    def when_col(self, obj):
        now = timezone.now()
        if obj.state == Job.QUEUED and obj.available_at > now:
            secs = int((obj.available_at - now).total_seconds())
            return format_html(
                '<span style="color:#b06000">{}</span>',
                _("in %(secs)ss") % {"secs": secs},
            )
        if not obj.mtime:
            return "—"
        secs = int((now - obj.mtime).total_seconds())
        if secs < 60:
            return _("%(n)ss ago") % {"n": secs}
        if secs < 3600:
            return _("%(n)smin ago") % {"n": secs // 60}
        return _("%(n)sh ago") % {"n": secs // 3600}

    @admin.display(description=_("worker"))
    def worker_col(self, obj):
        return _worker_link(obj.worker_id, short=True)

    @admin.display(description=_("worker"))
    def worker_link(self, obj):
        return _worker_link(obj.worker_id)

    @admin.display(description=_("parameters"))
    def params_col(self, obj):
        args = obj.args or []
        kwargs = obj.kwargs or {}
        if not args and not kwargs:
            return format_html(
                '<span style="color:#777">{}</span>', _("no parameters")
            )

        rows = [(f"[{i}]", v) for i, v in enumerate(args)] + sorted(
            kwargs.items()
        )
        body = "".join(
            format_html(
                "<tr>"
                '<td style="padding:4px 12px 4px 0;vertical-align:top;'
                'color:#555;white-space:nowrap"><code>{}</code></td>'
                '<td style="padding:4px 0"><code>{}</code></td>'
                "</tr>",
                name,
                json.dumps(value, ensure_ascii=False, default=str),
            )
            for name, value in rows
        )
        return format_html(
            '<table style="border-collapse:collapse;font-size:12px">{}</table>',
            mark_safe(body),
        )

    @admin.display(description=_("result"))
    def result_col(self, obj):
        res = Result.objects.filter(pk=obj.message_id).first()
        if res is None:
            return format_html(
                '<span style="color:#777">{}</span>',
                _("no stored result (actor has no store_results=True)"),
            )
        if res.is_failure:
            kind, msg = res.error
            return format_html(
                '<div style="background:#fce8e6;color:#d93025;padding:10px;'
                'border-radius:4px"><b>{}</b>: {}</div>',
                kind,
                msg,
            )
        return format_html(
            '<pre style="background:#e6f4ea;padding:10px;border-radius:4px;'
            'white-space:pre-wrap;margin:0">{}</pre>',
            json.dumps(res.payload, ensure_ascii=False, indent=2, default=str),
        )

    @admin.display(description=_("traceback"))
    def traceback_col(self, obj):
        if not obj.traceback:
            return "—"
        return format_html(
            '<pre style="white-space:pre-wrap;background:#fce8e6;'
            "padding:12px;border-radius:4px;max-height:400px;"
            'overflow:auto;margin:0">{}</pre>',
            obj.traceback,
        )

    # ---- overview on top of the changelist ------------------------------

    def changelist_view(self, request, extra_context=None):
        extra_context = extra_context or {}
        extra_context["dramatiq_summary"] = self._summary_html()
        return super().changelist_view(request, extra_context)

    def _summary_html(self):
        rows = (
            Job.objects.values("queue_name", "state")
            .annotate(n=Count("message_id"))
            .order_by("queue_name")
        )
        per_queue: dict[str, dict[str, int]] = {}
        totals = {Job.QUEUED: 0, Job.CONSUMED: 0, Job.REJECTED: 0}
        for r in rows:
            # "<queue>.DQ" is dramatiq's internal delay queue: a delayed job
            # waits there until due. Count it under the queue it came from.
            name = r["queue_name"].removesuffix(".DQ")
            q = per_queue.setdefault(
                name, {Job.QUEUED: 0, Job.CONSUMED: 0, Job.REJECTED: 0}
            )
            if r["state"] in q:
                q[r["state"]] += r["n"]
                totals[r["state"]] += r["n"]

        cards = _join(
            _card(label, totals[state], STATE_COLORS[state])
            for label, state in (
                (_("Queued"), Job.QUEUED),
                (_("Running"), Job.CONSUMED),
                (_("Failed"), Job.REJECTED),
            )
        )

        if per_queue:
            body = _join(
                format_html(
                    '<tr><td class="q">{}</td><td>{}</td><td>{}</td>'
                    "<td>{}</td></tr>",
                    q,
                    self._cell(c[Job.QUEUED], "#1a73e8"),
                    self._cell(c[Job.CONSUMED], "#188038"),
                    self._cell(c[Job.REJECTED], "#d93025"),
                )
                for q, c in sorted(per_queue.items())
            )
            table = format_html(
                '<table class="dq-summary"><thead><tr><th>{}</th><th>{}</th>'
                "<th>{}</th><th>{}</th></tr></thead><tbody>{}</tbody>"
                "<tfoot><tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>"
                "</tfoot></table>",
                _("Queue"),
                _("Queued"),
                _("Running"),
                _("Failed"),
                body,
                _("Total"),
                self._cell(totals[Job.QUEUED], "#1a73e8"),
                self._cell(totals[Job.CONSUMED], "#188038"),
                self._cell(totals[Job.REJECTED], "#d93025"),
            )
        else:
            table = format_html(
                "<p style='margin:12px 0 0;color:#777'>{}</p>",
                _("No jobs in the queue."),
            )

        return format_html(
            "<style>{}</style><div class='dq-wrap'>"
            "<div class='dq-cards'>{}</div>{}</div>",
            mark_safe(SUMMARY_CSS),
            cards,
            table,
        )

    @staticmethod
    def _cell(n, color):
        if not n:
            return format_html('<span class="zero">0</span>')
        return format_html('<b style="color:{}">{}</b>', color, n)

    # ---- actions -------------------------------------------------------

    @admin.action(description=_("Requeue selected jobs"))
    def action_retry(self, request, queryset):
        broker = dramatiq.get_broker()
        done = skipped = 0

        for job in queryset:
            if job.is_running:
                skipped += 1
                continue
            payload = dict(job.message or {})
            if not payload.get("actor_name"):
                skipped += 1
                continue
            # Reset the counter so the job gets its full retry cycle again.
            options = dict(payload.get("options") or {})
            options.pop("retries", None)
            options.pop("traceback", None)
            msg: dramatiq.Message = dramatiq.Message(
                queue_name=payload.get("queue_name") or job.queue_name,
                actor_name=payload["actor_name"],
                args=tuple(payload.get("args") or ()),
                kwargs=payload.get("kwargs") or {},
                options=options,
                message_id=payload.get("message_id") or str(job.message_id),
                message_timestamp=payload.get("message_timestamp")
                or current_millis(),
            )
            Job.objects.filter(pk=job.pk).delete()
            broker.enqueue(msg)
            done += 1

        if done:
            self.message_user(
                request,
                ngettext("%d job requeued.", "%d jobs requeued.", done) % done,
                messages.SUCCESS,
            )
        if skipped:
            self.message_user(
                request,
                ngettext(
                    "%d skipped: running or missing an actor.",
                    "%d skipped: running or missing an actor.",
                    skipped,
                )
                % skipped,
                messages.WARNING,
            )

    @admin.action(description=_("Discard selected jobs"))
    def action_discard(self, request, queryset):
        running = queryset.filter(state=Job.CONSUMED).count()
        deletable = queryset.exclude(state=Job.CONSUMED)
        n = deletable.count()
        deletable.delete()

        if n:
            self.message_user(
                request,
                ngettext("%d job discarded.", "%d jobs discarded.", n) % n,
                messages.SUCCESS,
            )
        if running:
            self.message_user(
                request,
                ngettext(
                    "%d skipped: a running job cannot be discarded.",
                    "%d skipped: running jobs cannot be discarded.",
                    running,
                )
                % running,
                messages.WARNING,
            )

    # ---- permissions ---------------------------------------------------

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        # The actions handle removal, with the state guard.
        return False


@admin.register(Worker)
class WorkerAdmin(admin.ModelAdmin):
    list_display = ("worker_id", "heartbeat_at", "status_badge", "jobs_col")
    ordering = ("-heartbeat_at",)

    fields = (
        "worker_id",
        "status_badge",
        "heartbeat_at",
        "heartbeat_age",
        "running_jobs",
    )
    readonly_fields = fields

    @admin.display(description=_("status"))
    def status_badge(self, obj):
        deadline = timezone.now() - datetime.timedelta(
            seconds=_heartbeat_ttl()
        )
        if obj.heartbeat_at < deadline:
            return _badge(_("dead"), "#d93025", "#fce8e6")
        return _badge(_("alive"), "#188038", "#e6f4ea")

    @admin.display(description=_("running jobs"))
    def jobs_col(self, obj):
        n = Job.objects.filter(worker_id=obj.worker_id).count()
        if not n:
            return "—"
        url = reverse("admin:dramatiq_postgres_job_changelist")
        return format_html(
            '<a href="{}?worker_id={}">{}</a>', url, obj.worker_id, n
        )

    @admin.display(description=_("last heartbeat"))
    def heartbeat_age(self, obj):
        secs = int((timezone.now() - obj.heartbeat_at).total_seconds())
        ttl = int(_heartbeat_ttl())
        human = (
            _("%(n)ss ago") % {"n": secs}
            if secs < 120
            else _("%(n)smin ago") % {"n": secs // 60}
        )
        if secs > ttl:
            return format_html(
                '<span style="color:#d93025"><b>{}</b> — {}</span>',
                human,
                _("past the %(ttl)ss TTL; this worker's jobs will be requeued")
                % {"ttl": ttl},
            )
        return format_html(
            '<span style="color:#188038">{}</span> {}',
            human,
            _("(TTL %(ttl)ss)") % {"ttl": ttl},
        )

    @admin.display(description=_("running jobs"))
    def running_jobs(self, obj):
        jobs = list(
            Job.objects.filter(worker_id=obj.worker_id).order_by(
                "-consumed_at"
            )[:50]
        )
        if not jobs:
            return format_html(
                '<span style="color:#777">{}</span>', _("no running jobs")
            )

        now = timezone.now()
        rows = _join(
            format_html(
                '<tr><td class="q"><a href="{}">{}</a></td><td>{}</td>'
                "<td>{}</td></tr>",
                reverse(
                    "admin:dramatiq_postgres_job_change", args=[str(j.pk)]
                ),
                j.actor_name or str(j.pk)[:8],
                j.queue_name,
                (
                    _("%(n)ss")
                    % {"n": int((now - j.consumed_at).total_seconds())}
                    if j.consumed_at
                    else "—"
                ),
            )
            for j in jobs
        )
        return format_html(
            "<style>{}</style>"
            '<table class="dq-summary"><thead><tr><th>{}</th><th>{}</th>'
            "<th>{}</th></tr></thead><tbody>{}</tbody></table>",
            mark_safe(SUMMARY_CSS),
            _("Actor"),
            _("Queue"),
            _("Held for"),
            rows,
        )

    def changelist_view(self, request, extra_context=None):
        extra_context = extra_context or {}
        extra_context["dramatiq_summary"] = self._summary_html()
        return super().changelist_view(request, extra_context)

    def _summary_html(self):
        deadline = timezone.now() - datetime.timedelta(
            seconds=_heartbeat_ttl()
        )
        alive = Worker.objects.filter(heartbeat_at__gte=deadline).count()
        dead_qs = Worker.objects.filter(heartbeat_at__lt=deadline)
        dead = dead_qs.count()

        # Jobs marked running but held by a worker that already died: nobody
        # is executing them. Broker maintenance requeues them.
        orphans = Job.objects.filter(
            state=Job.CONSUMED,
            worker_id__in=list(dead_qs.values_list("worker_id", flat=True)),
        ).count()

        cards = _join(
            _card(label, n, colors)
            for label, n, colors in (
                (_("Alive"), alive, ("#188038", "#e6f4ea")),
                (_("Dead"), dead, ("#d93025", "#fce8e6")),
                (_("Orphan jobs"), orphans, ("#b06000", "#fef7e0")),
            )
        )

        note = ""
        if orphans:
            note = format_html(
                "<p style='margin:12px 0 0;color:#b06000;font-size:13px'>{}</p>",
                ngettext(
                    "%(n)d job stuck on a dead worker — broker maintenance "
                    "requeues it within %(ttl)ds.",
                    "%(n)d jobs stuck on dead workers — broker maintenance "
                    "requeues them within %(ttl)ds.",
                    orphans,
                )
                % {"n": orphans, "ttl": int(_heartbeat_ttl())},
            )

        return format_html(
            "<style>{}</style><div class='dq-wrap'>"
            "<div class='dq-cards'>{}</div>{}</div>",
            mark_safe(SUMMARY_CSS),
            cards,
            note,
        )

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


@admin.register(Result)
class ResultAdmin(admin.ModelAdmin):
    """Stored actor results, for actors declared with ``store_results=True``.

    Outlives the job it came from: the queue row is deleted on ack, so this is
    where a completed task's return value remains visible.
    """

    list_display = ("message_id", "outcome", "expires_at")
    search_fields = ("message_id",)
    ordering = ("-expires_at",)
    fields = ("message_id", "outcome", "value", "expires_at")
    readonly_fields = fields

    @admin.display(description=_("outcome"))
    def outcome(self, obj):
        if obj.is_failure:
            kind, _msg = obj.error
            return _badge(kind or _("failed"), "#d93025", "#fce8e6")
        return _badge(_("ok"), "#188038", "#e6f4ea")

    @admin.display(description=_("value"))
    def value(self, obj):
        if obj.is_failure:
            kind, msg = obj.error
            return format_html(
                '<div style="background:#fce8e6;color:#d93025;padding:10px;'
                'border-radius:4px"><b>{}</b>: {}</div>',
                kind,
                msg,
            )
        return format_html(
            '<pre style="background:#e6f4ea;padding:10px;border-radius:4px;'
            'white-space:pre-wrap;margin:0">{}</pre>',
            json.dumps(obj.payload, ensure_ascii=False, indent=2, default=str),
        )

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False
