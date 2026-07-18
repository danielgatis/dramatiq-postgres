from django.conf import settings
from django.db import migrations

from dramatiq_postgres import schema


def forwards(apps, schema_editor):
    connection = schema_editor.connection
    if connection.vendor != "postgresql":
        raise RuntimeError(
            "dramatiq-postgres requires a PostgreSQL database, "
            f"got {connection.vendor!r}."
        )

    options = getattr(settings, "DRAMATIQ_BROKER", {}).get("OPTIONS", {})
    with connection.cursor() as curs:
        schema.init(
            curs,
            schema=options.get("schema") or "dramatiq",
            prefix=options.get("prefix") or "",
        )


class Migration(migrations.Migration):
    initial = True

    operations = [
        migrations.RunPython(forwards, migrations.RunPython.noop),
    ]
