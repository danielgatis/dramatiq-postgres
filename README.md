# dramatiq-pg − Postgres Broker for Dramatiq

[dramatiq](https://dramatiq.io/) is a simple task queue implementation for
Python3. dramatiq-pg provides a Postgres-based implementation of a dramatiq
broker.

**The project is not feature complete yet.**

## Features

- Super simple deployment.
- Message payload stored as native JSONb.
- All messages in a single table.
- All data are wrapped in a dedicated schema.
- Uses LISTEN/NOTIFY to keep worker sync. No polling.
- Reliable thanks to Postgres MVCC.
- Using plain psycopg2. No ORM.
- Requeueing of failed tasks.


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
setup](https://dramatiq.io/guide.html).


## Roadmap

- Rejecting message.
- Process missed notifies while resuming worker.
- Result storage as JSONb.
- Delayed task.
