# dramatiq-pg − Postgres Broker for Dramatiq

[dramatiq](https://dramatiq.io/) is a simple task queue implementation for
Python3. dramatiq-pg provides a Postgres-based implementation of a dramatiq
broker.

**The project is not feature complete yet.**

## Features

- Super simple deployment.
- Uses plain psycopg2. No ORM.
- Stores message payload as native JSONb.
- Stores all messages in a single table, in a dedicated schema.
- Uses LISTEN/NOTIFY to keep worker sync. No polling.
- Replay pending messages on worker startup.
- Requeues failed tasks.
- Reliable thanks to Postgres MVCC.
- Self-healing. Old messages are purge from time to time.

Note that dramatiq assumes tasks are idempotent. This broker makes the same
assumptions for recovering after a crash.


## Installation

- Install dramatiq-pg package from PyPI:
  ``` console
  $ pip install dramatiq-pg
  ```
- Apply dramatiq\_pg/schema.sql file in your database:
  ``` console
  $ psql -f dramatiq_pg/schema.sql
  ```
- Before importing actors, define global broker with a connection
  pool:
  ``` python
  import dramatiq
  import dramatiq_pg
  import psycopg2.pool

  pool = psycopg2.pool.ThreadedConnectionPool(0, 4, conninfo)
  dramatiq.set_broker(dramatiq_pg.PostgresBroker(pool=pool))
  ```

Now declare/import actors and manage worker just like any [dramatiq
setup](https://dramatiq.io/guide.html). An [example
script](https://gitlab.com/dalibo/dramatiq-pg/blob/master/example.py) is
available, tested on CI.

The CLI tool `dramatiq-pg` allows you to show stats on the queue and purge old
messages. See `--help` for details.


## Roadmap

- Result storage as JSONb.
- Delayed task.


Feel free to suggest feature through support channels.


## Support

If you encounter a bug or miss a feature, please [open an issue on
GitLab](https://gitlab.com/dalibo/dramatiq-pg/issues/new) with as much
information as possible.

dramatiq_pg is available under the PostgreSQL licence.
