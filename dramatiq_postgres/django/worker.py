"""Dramatiq worker entrypoint for Django projects.

Point the dramatiq CLI at this module::

    $ dramatiq dramatiq_postgres.django.worker

It boots Django (which configures the broker, see ``apps.py``) then imports
the actors module of every installed app. Override the module name with the
``DRAMATIQ_ACTORS_MODULE`` setting (default: ``actors``).
"""

import django
from django.conf import settings
from django.utils.module_loading import autodiscover_modules

django.setup()

autodiscover_modules(getattr(settings, "DRAMATIQ_ACTORS_MODULE", "actors"))
