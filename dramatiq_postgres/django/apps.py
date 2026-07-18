import dramatiq
from django.apps import AppConfig
from django.conf import settings
from django.utils.module_loading import import_string

from ..broker import PostgresBroker

# Django DATABASES keys mapped to libpq connection keywords.
_DB_KEYS = (
    ("NAME", "dbname"),
    ("USER", "user"),
    ("PASSWORD", "password"),
    ("HOST", "host"),
    ("PORT", "port"),
)


def connection_kwargs(alias="default"):
    """Build libpq connection kwargs from a Django database alias."""
    db = settings.DATABASES[alias]
    return {
        libpq: str(db[django]) for django, libpq in _DB_KEYS if db.get(django)
    }


class DramatiqPostgresConfig(AppConfig):
    name = "dramatiq_postgres.django"
    label = "dramatiq_postgres"
    verbose_name = "Dramatiq Postgres"

    broker = None

    def ready(self):
        config = getattr(settings, "DRAMATIQ_BROKER", {})

        encoder = config.get("ENCODER")
        if encoder:
            dramatiq.set_encoder(import_string(encoder)())

        options = dict(config.get("OPTIONS", {}))
        middleware = [
            import_string(m)() if isinstance(m, str) else m
            for m in config.get("MIDDLEWARE", [])
        ]
        if middleware:
            options["middleware"] = middleware

        if "pool" not in options and "url" not in options:
            alias = config.get("DATABASE_ALIAS", "default")
            options["url"] = connection_kwargs(alias)

        self.broker = PostgresBroker(**options)
        dramatiq.set_broker(self.broker)
