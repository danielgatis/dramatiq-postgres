==================
 Deployment Guide
==================

Dramatiq-pg implements broker logic in application process. Postgres is only
responsible of the storage and the inter-client notifications. There is no
additionnal service to maintain.

However, you have to setup properly the application and to keep Postgres healthy
as usual.


Application Setup
=================

Your application is likely to use Postgres for it's business data. However,
using Postgres as a broker has some limitation and you should not configure
Dramatiq-pg like application code. The Dramatiq-pg configuration must conform
with the following limitations:

- Postgres server must be primary, not standby. Both producer and consumer
  writes in the message table.
- Postgres emits notify only to connection on the same server. Postgres does not
  replicate notify.
- If you use pgbouncer, you must configure session pooling method to keep
  notify. Dramatiq-pg already use a client-side connection pool. You won't
  benefit of advanced feature of pgbouncer to reduce connection to Postgres
  server.

Each dramatiq worker process opens one persistent connection per queue and one
connection to ack messages. Thus, to be safe, you should provision worker pool
size with ``num_processes x num_queues x 2``. A best practice is to keep process
count low and reduce the number of queues.

The application consume a non-persistent connection to emit the message. When
application stores task result, a connection is consumed in Dramatiq-pg
connection pool to wait for and fetch the task result. There is no persistent
connection.


Monitoring
==========

Dramatiq has `Prometheus support built-in
<https://dramatiq.io/advanced.html#prometheus-metrics>`_. Dramatiq-pg does
**not** adds metrics to your regular Postgres monitoring.

The ``dramatiq-pg`` CLI tool has a ``stats`` command that output some metric.

::

   $ dramatiq-pg status
   queued: 0
   consumed: 0
   done: 3
   rejected: 0


The ``dramatiq-pg`` CLI tool is only configured using ``PG*`` env vars.


Troubleshooting
===============

When a worker process crashes in the middle of a task, the message is not
replayed automatically. If you don't replay it, it will never be processed
completely. Use ``dramatiq-pg recover`` to requeue consumed message. The
``--minage`` parameter may help you to avoid requeue message consumed by running
worker. ``--minage`` accepts a Postgres interval value.

::

   dramatiq-pg recover --minage 5m

Assuming the crash occured 5 minutes ago, this command requeues messages
consumed 5 minutes ago and beyond, excluding messages consumed between 5 minutes
ago and now.

Note that Dramatiq assumes tasks are idempotent. Thus, requeueing a processing
task should not be an issue.


Flushing
--------

You can flush all queues, including queued and consumed messages by using
``dramatiq-pg flush`` command. All messages are lost.


Queue Maintainance
==================

Dramatiq-pg tries to be self-healing, even without dedicated service. Worker
randomly purge queues from message older than 30 days. Automatic purge triggers
daily per worker.

You can trigger manually a purge of old messages by calling ``dramatiq-pg
purge``. This command accepts a ``--maxage`` argument with a Postgres interval
value. All message marked as ``done`` or ``rejected`` and older than ``maxage``
will be dropped.

You may have some bloat in queue table. Configure Postgres auto vacuum and
monitoring to keep bloat under control.
