# dramatiq-postgres

Postgres broker for [Dramatiq](https://dramatiq.io/). Your task queue lives
in the database you already have — no Redis, no RabbitMQ.

## Install

```console
$ pip install dramatiq-postgres psycopg2-binary
```

## Usage

Create the schema (idempotent, safe to run on every deploy):

```console
$ dramatiq-postgres init
```

Declare the broker and your actors:

```python
# tasks.py
import dramatiq
from dramatiq_postgres import PostgresBroker

dramatiq.set_broker(PostgresBroker(url="postgresql:///mydb"))

@dramatiq.actor
def hello(name):
    print(f"hello {name}")
```

Send messages from anywhere:

```python
hello.send("world")
hello.send_with_options(args=("later",), delay=60_000)  # in a minute
```

Run workers:

```console
$ dramatiq tasks
```

That's it. Results are built in too:

```python
@dramatiq.actor(store_results=True)
def add(a, b):
    return a + b

message = add.send(2, 2)
message.get_result(block=True)  # 4
```

## Django

```console
$ pip install dramatiq-postgres[django]
```

1. Add `dramatiq_postgres.django` to `INSTALLED_APPS`.
2. Run `manage.py migrate` — the queue schema is created by a regular
   Django migration.
3. Declare actors in an `actors.py` module inside your apps.
4. Start workers:

```console
$ dramatiq dramatiq_postgres.django.worker
```

The broker connects using your `default` database automatically. To
customize, declare a `DRAMATIQ_BROKER` setting:

```python
DRAMATIQ_BROKER = {
    "OPTIONS": {},        # PostgresBroker kwargs; url/pool default to DATABASES
    "MIDDLEWARE": [],     # dotted paths or instances of extra middleware
    "ENCODER": None,      # dotted path of a dramatiq encoder class
    "DATABASE_ALIAS": "default",
}
```

## Configuration

All `PostgresBroker` options:

| Option                 | Default     | Description                                                        |
| ---------------------- | ----------- | ------------------------------------------------------------------ |
| `url`                  | `""`        | libpq URL or kwargs dict; `?maxconn=16` caps the pool              |
| `pool`                 | `None`      | bring your own psycopg2 pool instead of `url`                      |
| `results`              | `True`      | enable the result backend and middleware                           |
| `schema`               | `dramatiq`  | Postgres schema holding the tables                                 |
| `prefix`               | `""`        | table name prefix                                                  |
| `listen`               | `True`      | LISTEN for instant delivery; set `False` behind pgbouncer          |
| `notify`               | `True`      | NOTIFY on enqueue; set `False` for maximum enqueue throughput      |
| `poll_interval`        | `1.0`       | seconds between polls (the safety net, or the only source of wake-ups with `listen=False`) |
| `heartbeat_interval`   | `15.0`      | seconds between worker heartbeats                                  |
| `heartbeat_ttl`        | `60.0`      | seconds without heartbeat before a worker is considered dead       |
| `maintenance_interval` | `30.0`      | seconds between maintenance runs                                   |
| `purge_maxage`         | `"30 days"` | how long rejected messages are kept                                |

The CLI ships maintenance commands, all honoring `--dsn`, `--schemaname`
and `--prefix`:

```console
$ dramatiq-postgres init      # create the schema if missing
$ dramatiq-postgres stats     # message counts by state
$ dramatiq-postgres recover   # requeue stuck consumed messages
$ dramatiq-postgres flush     # delete queued/consumed messages
$ dramatiq-postgres purge     # delete old rejected messages
```

## How it works

Everything is plain Postgres — three tables and LISTEN/NOTIFY. No
extension, no ORM, no extra service.

**Enqueue.** `send()` INSERTs the message as JSONB into the `queue` table
and fires a `NOTIFY` on `dramatiq.<queue>.enqueue` with an empty payload.
The NOTIFY is just a doorbell: it wakes workers up, it carries no data.

**Claim.** Each worker polls with one round trip: a batch of due messages
is claimed with `FOR UPDATE SKIP LOCKED`, ordered by `available_at` then
`position` (FIFO). Workers never race for the same row and never block
each other. A partial index covers exactly the `state = 'queued'` rows, so
the claim stays fast no matter how large the table gets.

**Delivery.** With `listen=True` (default), one shared LISTEN connection
per worker process turns enqueues into instant wake-ups; the
`poll_interval` is only a safety net. With `listen=False` (needed behind
pgbouncer in transaction pooling mode), workers rely on polling alone.

**Delayed messages.** `delay=` writes a future `available_at`. Scheduling
lives server-side in the table — nothing is held in worker memory, so
restarts never lose scheduled work.

**Ack / results.** Acknowledging a message DELETEs its row — the hot table
only ever contains pending and in-flight work. Actor results go to the
separate `result` table with a TTL.

**Failures.** A message that exhausts its retries is kept with
`state = 'rejected'` for inspection, and purged after `purge_maxage`.

**Crash recovery.** Every worker upserts a heartbeat row each
`heartbeat_interval`. One worker at a time (elected via advisory lock, every
`maintenance_interval`) requeues messages owned by workers whose heartbeat
expired, deletes stale worker rows, and purges old rejected messages and
expired results. Kill -9 a worker and its messages are back in the queue
within `heartbeat_ttl` seconds — no manual intervention.

### Tables

All in the `dramatiq` schema (configurable via `schema`/`prefix`):

**`queue`** — pending and in-flight messages:

| Column        | Type          | Description                                    |
| ------------- | ------------- | ---------------------------------------------- |
| `message_id`  | `uuid` PK     | Dramatiq message id                            |
| `queue_name`  | `text`        | queue the message belongs to                   |
| `state`       | `enum`        | `queued`, `consumed` or `rejected`             |
| `message`     | `jsonb`       | the message payload, as encoded by Dramatiq    |
| `position`    | `bigint`      | monotonic enqueue counter, FIFO tie-breaker    |
| `available_at`| `timestamptz` | do not deliver before this moment (delay/eta)  |
| `worker_id`   | `uuid`        | worker owning the message while consumed       |
| `consumed_at` | `timestamptz` | when the message was claimed                   |
| `mtime`       | `timestamptz` | last state change                              |

**`worker`** — one row per live worker process:

| Column         | Type          | Description                          |
| -------------- | ------------- | ------------------------------------ |
| `worker_id`    | `uuid` PK     | worker identity, one per process     |
| `heartbeat_at` | `timestamptz` | last heartbeat                       |

**`result`** — actor results, decoupled from the queue:

| Column       | Type          | Description                     |
| ------------ | ------------- | ------------------------------- |
| `message_id` | `uuid` PK     | message the result belongs to   |
| `result`     | `jsonb`       | encoded actor return value      |
| `expires_at` | `timestamptz` | TTL for automatic purge         |

Connection budget per worker process: the broker pool (up to `maxconn`,
default 16) plus one LISTEN connection.

## Support

If you find this project useful, consider buying me a coffee (or a beer):

<a href="https://www.buymeacoffee.com/danielgatis" target="_blank"><img src="https://bmc-cdn.nyc3.digitaloceanspaces.com/BMC-button-images/custom_images/orange_img.png" alt="Buy Me A Coffee" style="height: auto !important;width: auto !important;"></a>

## License

Copyright (c) 2026-present [Daniel Gatis](https://github.com/danielgatis)

Licensed under the [MIT License](./LICENSE).
