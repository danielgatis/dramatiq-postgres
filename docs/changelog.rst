Dramatiq-pg Changelog
=====================

Unreleased
----------

- Automatic recovery of message after crash. You don't need to manually requeue
  anymore.
- More reliability: connection lost are handled everywhere, retrying on network
  failure is enabled.
- Allows to use psycopg2-binary wheel. You must install psycopg2 or
  psycopg2-binary yourself.
- By default, connection pool tries to reuse all connections.
- dramatiq.queue table definition has been reviewed for optimisation. Changes
  are not required.


Version 0.5.0
-------------

Released 2019-04-04.

This release requires an update of the schema.

- Stores result in Database. This is enabled by default.
- Flush all queues from CLI.
- Documentation user guide, deployment, the why.
- Add performance metric tools.


Version 0.4.0
-------------

Released 2019-03-13.

-  Fixed blocking consumer thread. ``select`` syscall is now called
   every seconds by default.
-  Removed automatic recovery on startup. This break multi-worker
   process on same queue with long running task. You need to manually
   requeue messages after a crash.
-  Added delayed task support.
-  Added documentation on deployment constaints and limitations.
-  Added manual requeue from CLI tool.
-  Added URL parameter to PostgresBroker constructor.
-  Reuse listening connexion to purge message table. This reduce slighly
   connexion usage.


Version 0.3.0
-------------

Released 2019-03-07.

-  Added rejecting message (nack).
-  Added message replay from table at startup. Missed NOTIFY are not
   lost anymore.
-  Requeue old consumed message on startup. Recover from crashed
   process.
-  Added CLI tool to manually purge queue and show some stats.
-  Added random periodic purge of message table.
-  Use BIGSERIAL on message table.
-  Added index on message table to fasten purge and stats.
-  Added projet licence, logo and metadata.


Version 0.2.0
-------------

Released 2019-02-22.

-  First working implementation.
-  Added func tests.
