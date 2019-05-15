# dramatiq-pg − Postgres Broker for Dramatiq

[Dramatiq](https://dramatiq.io/) is a simple task queue implementation for
Python3. dramatiq-pg provides a Postgres-based implementation of a dramatiq
broker.

## Features

- Super simple deployment.
- Uses plain psycopg2. No ORM.
- Stores message payload and results as native JSONb.
- Stores all messages in a single table, in a dedicated schema.
- Uses LISTEN/NOTIFY to keep worker sync. No polling.
- Requeue of failed tasks.
- Delayed task.
- Reliable thanks to Postgres MVCC.
- Self-healing. Old messages are purge from time to time.
- Utility CLI for maintainance : flush, purge, stats, etc.

Note that dramatiq assumes tasks are idempotent. This broker makes the same
assumptions for recovering after a crash.


## Installation

- Install dramatiq-pg package from PyPI:
  ``` console
  $ pip install dramatiq-pg psycopg2-binary
  ```
  Ensure you have either psycopg2 or psycopg2-binary installed.
- Apply dramatiq\_pg/schema.sql file in your database:
  ``` console
  $ psql -f dramatiq_pg/schema.sql
  ```
- Before importing actors, define global broker with a connection
  pool:
  ``` python
  import dramatiq
  import psycopg2.pool
  from dramatiq_pg import PostgresBroker

  dramatiq.set_broker(PostgresBroker(i))

  @dramatiq.actor
  def myactor():
      ...
  ```

Now declare/import actors and manage worker just like any [dramatiq
setup](https://dramatiq.io/guide.html). An [example
script](https://gitlab.com/dalibo/dramatiq-pg/blob/master/example.py) is
available, tested on CI.

The CLI tool `dramatiq-pg` allows you to requeue messages, purge old messages
and show stats on the queue. See `--help` for details.

[Dramatiq-pg
documentation](https://gitlab.com/dalibo/dramatiq-pg/blob/master/docs/index.rst)
is hosted on GitLab and give you more details on deployment and operation of
Postgres as a Dramatiq broker.


## Support

If you encounter a bug or miss a feature, please [open an issue on
GitLab](https://gitlab.com/dalibo/dramatiq-pg/issues/new) with as much
information as possible.

dramatiq_pg is available under the PostgreSQL licence.
