"""Django integration for dramatiq-postgres.

Add ``dramatiq_postgres.django`` to ``INSTALLED_APPS`` and run ``manage.py
migrate`` to initialize the schema. The broker is configured from the
``DRAMATIQ_BROKER`` setting, and defaults to the ``default`` Django
database. Start workers with ``dramatiq dramatiq_postgres.django.worker``.
"""
