================
 Why Postgres ?
================

Using Postgres as a message broker may look odd. There is some reason to use
Postgres as a message broker and some to use something else. This page gives a
few element to make the best choice.

Let's start with some generalities. Webservice delegating tasks to background
service is far from new as a software architecture pattern. We can resume the
requirements as :

- Send message asynchronously from webservice to backoffice.
- Retrieve message from backoffice process.
- Send back the result of backoffice for webservice.

Most applications in such architecture use a relationnal database and Postgres
is the best open-source choice. The simplest solution is to put messages in a
table and start a backoffice process with a cron. The backoffice eats messages
from the table, do the work and store the result in the same database.


From cron to message queue
==========================

What if you want a faster processing of background task? Here comes message
queueing and its dedicated protocol: Advanced Message Queue protocol.

The AMQP protocol is dedicated to just this: emit unstructured message in a
queue and deliver it to one consumer as soon as possible. RabbitMQ is the most
common open-source AMQP server.

However, AMQP does not provide a way to send back an unstructured message from
backoffice to webservice. The trends is to use a key-value store like REDIS or
Memcached. This lead to a rather complex architecture: a webservice, a database,
a message broker, a backoffice and a key-value store. Using REDIS as a message
broker is now common, it allows to avoid RabbitMQ which is quite heavy.


Consolidating Infrastructure
============================

In this complex architecture, the database is always the first foundation of the
app. The thing is that Postgres can check almost all of the feature list of both
message broker and key-value store:

- Storing unstructured message, thanks to JSON.
- Instant asynchronous remote notifaction, thanks to ``LISTEN`` and ``NOTIFY``.
- Ensuring persistence, that's the base of Postgres job.
- Ensuring reliability, Postgres MVCC and HA should do the job.

Thus there is no limitation to using Postgres as both a message broker and a
key-value store. The single limitation is to have a transparent implementation
of this pattern for various distributed task system. Dramatiq-pg offer an
implementation for Postgres.

Actually, Skype initiated an extension to Postgres for managing queues: `PgQ
<https://github.com/pgq/>`_. It's rather inactive as a project but may fit your
needs. Dramatiq-pg does not (yet) implement a Dramatiq broker backed by PgQ.


Performance
===========

The cost of emitting and processing a message delivered by Postgres is directly
bound to the cost of an INSERT or UPDATE in a single table with a few indexes.
Emitting a message costs one INSERT. Consuming and acknowledging a message each
costs one UPDATE. From time to time, you have a DELETE to purge old processed
messages. The cost of NOTIFY is light compared to the cost of INSERT, but
increases with the number of LISTEN. Storing result costs one UPDATE too.

For the curiousity, I measured the message rate that Postgres could handle on my
laptop. My laptop is a Thinkpad x260 with a i5 processor @ 2.3GHz, 2 cores, 2
threads per cores. It has 16Go of RAM and a 250Go **crypted** SSD. I run a
vanilla Postgres in a docker container, unoptimized. The test app has a noop
task with a single parameter, so the message size is quite small. Postgres,
emitter and worker runs on the same host, at the same time. Here are the key
metrics:

- Message emission rate: 190 message per seconds.
- Message processing rate: 91 message per seconds.

The difference between emission an processing is consistent with the cost of
INSERT/UPDATE. Processing costs twice as emitting as it implies two UPDATE while
emitting implies one INSERT.

Dramatiq-pg ships a perf.py script to measure the performance of you Postgres
instance and perfagg.py scripts to aggregate metrics. Knowing this metrics will
help you decide if Postgres fit your requirements.

There is more performance to measure like latency which depends on your network.
Also, the performance may change depending on the size of messages, the
replication setup, etc.

For an (unfair) idea of comparison, `RabbitMQ reaches thousands of message per
seconds
<https://www.rabbitmq.com/blog/2012/04/25/rabbitmq-performance-measurements-part-2/>`_
with a totally different stack: baremetal, bigiron, no disk encryption, etc.


Choosing the best option
========================

You have to balance between complexity of your infrastructure, the
multiplications of skills needed and the performance you need to fit your
application usage and workflow.

Overall, Postgres as a broker seems fair for simple application with low message
rate and a dedicated Postgres cluster.
