# dramatiq-pg − Postgres Broker for Dramatiq

[Dramatiq](https://dramatiq.io/) is a simple task queue implementation for
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
- Delayed task.
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
  import psycopg2.pool
  from dramatiq_pg import PostgresBroker

  dramatiq.set_broker(PostgresBroker(url="postgresql:///?minconn=0&maxconn=10"))

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


## Deployment

Postgres does not replicate notifications to standby instances. Thus the broker
connection pool must point to the master instance. Actor can connect to hot
standby for its work.

If you use pgbouncer, you must configure session pooling method to keep notify.

Each dramatiq process opens one persistent connection per queue and one
connection to ack messages. Thus, to be save, you should provision pool size
with `num_processes x num_queues x 2`. A best practice is to only add process as
needed and reduce the number of queues.


## Roadmap

- Result storage as JSONb.

Feel free to suggest feature through support channels.


## Support

If you encounter a bug or miss a feature, please [open an issue on
GitLab](https://gitlab.com/dalibo/dramatiq-pg/issues/new) with as much
information as possible.

dramatiq_pg is available under the PostgreSQL licence.
