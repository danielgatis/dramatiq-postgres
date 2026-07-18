#
#       P E R F O R M A N C E
#
#
# The goal of this script is to give a rough idea of how much message you can
# send and process per seconds.
#
# There is a concurrency between sending and processing. So, as long as sent
# messages count is greater than processed, this should give an idea.
#
# The strategy is to start worker process, start emitting threads and wait a
# fixed amount of time. After this delay, kill everything and count how much
# messages are in the table, by states.


import bdb
import csv
import logging
import os
import pdb
import signal
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timedelta
from queue import Queue
from textwrap import dedent
from threading import Barrier
from time import sleep

import dramatiq
from psycopg_pool import ConnectionPool

import dramatiq_postgres
from dramatiq_postgres.cli import transaction

logger = logging.getLogger(__name__)
# Empty connstring let's you configure psycopg using PG* env vars.
pool = ConnectionPool("", min_size=0, max_size=16, open=True)
# PostgresBroker accepts either pool= or url=. URL is a libpq connstring.
# PostgresBroker creates a ConnectionPool from URL, swallowing minconn
# and maxconn query argument.
dramatiq.set_broker(dramatiq_postgres.PostgresBroker(pool=pool))


@dramatiq.actor
def noop(*a, **kw):
    # Minimal task to limit performance bias.
    pass


def main(debug=True):
    # This is the main function, orchestrating each parts of the perf test.

    # First, setup test parameters.
    if debug:
        countdown = 1
        # Worker process/threads.
        wprocesses = 1
        wthreads = 1
        # Number of emitter threads.
        ethreads = 2
        mcount = 100
    else:
        countdown = 10
        wprocesses = 1
        wthreads = 4
        ethreads = 4
        # Having 500 message/s would be good.
        mcount = countdown * 500

    with transaction(pool) as curs:
        logger.info("Truncating message table.")
        curs.execute("TRUNCATE dramatiq.queue RESTART IDENTITY;")

    logger.info(
        "Launching dramatiq worker with %s processes and %s threads.",
        wprocesses,
        wthreads,
    )
    with worker(processes=wprocesses, threads=wthreads):
        logger.info("Processing messages for %ss.", countdown)
        logger.info("Emitting with %s threads.", ethreads)
        with Fixedtime(delay=countdown):
            emitter_main(threads=ethreads, count=mcount)
    logger.info("Execution time elapsed.")

    # Count messages sent / processed. Acknowledged messages are deleted
    # from the queue, so sent is read from the position identity sequence
    # and processed is whatever did not remain in the table.
    with transaction(pool) as curs:
        curs.execute(dedent("""\
        SELECT COALESCE((
            SELECT last_value FROM pg_sequences
            WHERE schemaname = 'dramatiq'
                AND sequencename = 'queue_position_seq'), 0);
        """))
        (sent,) = curs.fetchone()

        curs.execute("SELECT count(*) FROM dramatiq.queue;")
        (remaining,) = curs.fetchone()
        done = sent - remaining

    csvname = csvsave("sender", [1, ethreads, sent, countdown])
    logger.info(
        "Sent %s messages in %s. Saved in %s.", sent, countdown, csvname
    )

    csvname = csvsave("worker", [wprocesses, wthreads, done, countdown])
    logger.info(
        "Processed %s messages in %ss. Saved in %s.", done, countdown, csvname
    )


def emitter(b, q):
    # Main function for emitter thread. Sequencially emit count message.

    # Wait for start signal.
    b.wait()
    logger.debug("Starting to emit.")
    while not q.empty():
        q.get()
        noop.send(message="send from perf")
        q.task_done()
    logger.debug("Stopped emitting messages.")


class Timer(object):
    # A context manager timeing execution of with block.

    def __init__(self):
        self.delta = timedelta()
        self.start: datetime | None = None

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, self.delta)

    def __enter__(self):
        self.start = datetime.utcnow()
        return self

    def __exit__(self, exc_type, e, tb):
        assert self.start is not None
        self.last_delta = datetime.utcnow() - self.start
        self.delta += self.last_delta
        self.start = None


class Timeout(Exception):
    pass


class Fixedtime(object):
    # A context manager ensuring with block take a fixed time, padding with
    # sleep if needed.
    #
    # It is used to give a fixed time for worker to process as much messages as
    # possible while emitting messages in queue.

    def __init__(self, delay=10):
        self.delay = delay

    def alarm_handler(self, sig, stack_frame):
        raise Timeout()

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.alarm_handler)
        signal.alarm(self.delay)
        return self

    def __exit__(self, exc_type, *_):
        try:
            if exc_type is None:
                logger.info("Waiting to reach countdown of %ss.", self.delay)
                sleep(self.delay + 1)
        except Timeout:
            pass

        signal.alarm(0)
        signal.signal(signal.SIGALRM, signal.SIG_DFL)

        if exc_type is Timeout:
            # Swallow Timeout exceptions.
            return True


@contextmanager
def worker(processes=1, threads=4):
    # Manage the dramatiq worker process.

    proc = subprocess.Popen(
        [
            "dramatiq",
            "--processes",
            str(processes),
            "--threads",
            str(threads),
            "perf",
        ]
    )
    sleep(0.5)
    try:
        yield proc
    finally:
        logger.debug("Terminating dramatiq process.")
        proc.terminate()
        sleep(1)
        proc.kill()
        proc.communicate()


def emitter_main(threads=4, count=1000):
    # Manage emitter threads

    executor = ThreadPoolExecutor(
        max_workers=threads, thread_name_prefix="emitter"
    )

    # Prefill queue with count items.
    q: Queue[bool] = Queue(maxsize=count)
    for _ in range(count):
        q.put_nowait(True)

    b = Barrier(1 + threads)
    for _ in range(threads):
        executor.submit(emitter, b, q)

    try:
        # Triggers emitter's loop.
        b.wait()
        q.join()
    except Exception:
        logger.debug("Stopping emitters.")
        q.queue.clear()
        executor.shutdown(wait=True)
        raise


CSVSUFFIX = datetime.utcnow().strftime("%Y%m%dT%H%M%S")


def csvsave(name, row):
    # Helper to save metrics in CSV file for aggregation.
    fname = f"perf-{name}-{CSVSUFFIX}.csv"
    with open(fname, mode="w") as fo:
        writer = csv.writer(fo)
        writer.writerow([name] + row)
    return fname


if "__main__" == __name__:
    debug = "DEBUG" in os.environ
    logging.basicConfig(
        datefmt="%H:%M:%S",
        format="%(asctime)s [%(threadName)s] %(levelname)1.1s: %(message)s",
        level=logging.DEBUG if debug else logging.INFO,
    )

    try:
        exit(main(debug=debug))
    except (bdb.BdbQuit, KeyboardInterrupt):
        logger.exception("Interrupted.")
    except Exception:
        logger.exception("Unhandled error:")
        if debug and sys.stdout.isatty():
            logger.debug("Dropping in debugger.")
            pdb.post_mortem(sys.exc_info()[2])

    exit(os.EX_SOFTWARE)
