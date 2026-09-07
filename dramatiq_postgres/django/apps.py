import dramatiq
from django.apps import AppConfig
from dramatiq.middleware import default_middleware
from django.conf import settings
from django.utils.module_loading import import_string

from ..broker import PostgresBroker
from .middleware import DbConnectionsMiddleware

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
        # Workers are long-lived threads with no request cycle to close Django's
        # connections, so this is needed whatever else the user configures. An
        # empty list is left alone: Broker reads it as "use the defaults", and
        # overriding that would silently drop Retries & co.
        if middleware:
            if not any(isinstance(m, DbConnectionsMiddleware) for m in middleware):
                middleware.append(DbConnectionsMiddleware())
            options["middleware"] = middleware
        else:
            options["middleware"] = [
                m() for m in default_middleware
            ] + [DbConnectionsMiddleware()]

        if "pool" not in options and "url" not in options:
            alias = config.get("DATABASE_ALIAS", "default")
            options["url"] = connection_kwargs(alias)

        self.broker = PostgresBroker(**options)
        dramatiq.set_broker(self.broker)
