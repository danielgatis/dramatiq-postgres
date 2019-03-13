# Dramatiq-pg Changelog

## Unreleased changes

- Fixed blocking consumer thread. `select` syscall is now called every seconds
  by default.
- Removed automatic recovery on startup. This break multi-worker process on same
  queue with long running task. You need to manually requeue messages after a
  crash.
- Added delayed task support.
- Added documentation on deployment constaints and limitations.
- Added manual requeue from CLI tool.
- Added URL parameter to PostgresBroker constructor.
- Reuse listening connexion to purge message table. This reduce slighly
  connexion usage.


## 0.3.0 (2019-03-07)

- Added rejecting message (nack).
- Added message replay from table at startup. Missed NOTIFY are not lost
  anymore.
- Requeue old consumed message on startup. Recover from crashed process.
- Added CLI tool to manually purge queue and show some stats.
- Added random periodic purge of message table.
- Use BIGSERIAL on message table.
- Added index on message table to fasten purge and stats.
- Added projet licence, logo and metadata.


## 0.2.0 (2019-02-22)

- First working implementation.
- Added func tests.
