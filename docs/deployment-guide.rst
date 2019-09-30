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


Connection Usage
================

On top of regular connection usage for accessing your data, using Postgres as a
broker increase the needed connections. Dramatiq-pg's broker has its own pool of
connection. Your broker is used in different situation : application, worker,
scheduler, etc. Each will have it's own formula to determine its connection pool
size. The first step is to size these Dramatiq-pg connection pools properly. The
second step is to allocate enough connection in PostgreSQL with
``max_connection`` for all the pools.

Application Pool
----------------

The application pool is simple. Each thread of the application requires only one
connection at a time to either send messages or get back the result. Application
pool size equals ``num_threads``.

Note that a scheduler like `periodiq <https://gitlab.com/bersace/periodiq>`_
should be considered as a single threaded app.

Worker Pool
-----------

The Dramatiq worker pool size is slightly more complex to size. Each dramatiq
worker process opens **two** persistent connections per queue : one for
listening and one to consume/ack messages. Each worker thread requires a
connection to acknowledge message. Thus, to be save, you should size the pool
with ``num_queues x 2 + num_threads``.

Other Usage
-----------

Their is some more connections required for monitoring and eventually manage
queue with ``dramatiq-pg`` command. Consider each of these usage as a single
threaded application, consuming one connection.

Summarize All
-------------

On PostgreSQL side, you have to sum the size of all instanciated pools. Each
worker service can run several processes, defaulting to 8. This multiply the
number of required connection on PostgreSQL side. Also, you may require one or
more connection to monitor and manage the queue.

The final formula for allocating connection on PostgreSQL would be:

.. code::

   app_pool_size = app_threads
   worker_pool_size = num_queues * 2 + app_threads
   scheduler_pool_size = 1
   monitoring_pool_size = 1
   management_pool_size = 1

   max_connection = \
       app_processes * app_pool_size + \
       num_worker * worker_processes * worker_pool_size + \
       scheduler_pool_size + \
       monitoring_pool_size + \
       management_pool_size

For example, a regular web application including 1 app process with 4 threads, 1
worker service with 1 process and 2 threads, a scheduler and 2 queues results in
a connection usage of ``4 + 1 * (2 * 2 + 2) + 1 + 1 + 1`` or up to 13
connections used for messaging. Add 13 to you ``max_connection`` and you're
done.

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
