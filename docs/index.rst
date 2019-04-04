=============
 Dramatiq-pg
=============

Welcome to Dramatiq-pg documentation. Dramatiq-pg is a broker implementation of
Dramatiq_ backed by Postgres_ RDBMS. Dramatiq-pg is licensed under the
`PostgreSQL license`_.

Features
--------

- Super simple deployment.
- Uses plain psycopg2. No ORM.
- Stores message payload and results as native JSONb.
- Standalone result storage.
- Stores all messages in a single table, in a dedicated schema.
- Uses LISTEN/NOTIFY to keep worker sync. No polling.
- Requeue of failed tasks.
- Delayed task.
- Reliable thanks to Postgres MVCC.
- Self-healing. Old messages are purge from time to time.
- Utility CLI for maintainance : flush, purge, stats, etc.


Contents
--------

- `Get Started <get-started.rst>`_
- `User Guide <user-guide.rst>`_
- `Deployment Guide <deployment-guide.rst>`_
- `Why Postgres ? <why.rst>`_
- `Changelog <./changelog.rst>`_


Project Info
------------

- `Source Code <https://gitlab.com/dalibo/dramatiq-pg>`_
- `Issue tracker <https://gitlab.com/dalibo/dramatiq-pg/issues>`_
- `PostgreSQL License`_

.. _Dramatiq: https://dramatiq.io/
.. _Postgres: https://postgresql.org/
.. _PostgreSQL license: ../LICENSE
