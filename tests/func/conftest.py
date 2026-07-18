import os
import signal
import sys
from contextlib import contextmanager
from shutil import copyfileobj
from subprocess import Popen
from time import sleep

import psycopg
import pytest


class Listener(object):
    class Timeout(Exception):
        pass

    def __init__(self):
        self.conn = None

    def __enter__(self):
        self.conn = psycopg.connect("", autocommit=True)
        self.conn.execute('LISTEN "dramatiq.default.ack";')

    def __exit__(self, *_):
        assert self.conn is not None
        self.conn.close()
        self.conn = None

    def wait(self, count=1, timeout=8):
        assert self.conn is not None
        # Notifications received since LISTEN are queued in the connection
        # backlog and count as well.
        notifies = list(self.conn.notifies(timeout=timeout, stop_after=count))
        if len(notifies) < count:
            raise self.Timeout("Timeout")
        return notifies


@contextmanager
def pgconn_manager():
    # Commits on success, rolls back on error, then closes.
    with psycopg.connect("") as conn:
        with conn.cursor() as curs:
            yield curs


def truncate(table):
    with pgconn_manager() as curs:
        curs.execute(f"TRUNCATE {table};")


@pytest.fixture(autouse=True)
def pgconn():
    return pgconn_manager


@pytest.fixture(scope="session", autouse=True)
def flush_queue():
    truncate("dramatiq.queue")
    yield None


@pytest.fixture()
def witness():
    truncate("functest.witness")
    yield None


@pytest.fixture()
def listener():
    return Listener()


class WorkerManager(object):
    def __init__(self, name="workers"):
        self.logfilename = f"my-{name}.log"

    def start(self):
        logfo = self.open_log("w+")
        self.proc = Popen(
            [
                "dramatiq",
                "--verbose",
                "--log-file",
                self.logfilename,
                "--processes=4",
                "--threads=2",
                "tests.func.example",
            ],
            start_new_session=True,
        )
        with logfo:
            self.watch_log(logfo, needle="Worker process is ready")

    def stop(self, *_):
        self.proc.poll()
        if self.proc.returncode is None:
            self.proc.terminate()
            sleep(0.5)
            self.proc.poll()
        if self.proc.returncode is not None:
            self.proc.terminate()
        self.proc.communicate()

        sys.stdout.write("\n")
        with open(self.logfilename) as fo:
            copyfileobj(fo, sys.stdout)

    def crash(self):
        pgid = os.getpgid(self.proc.pid)
        os.killpg(pgid, signal.SIGKILL)

    def open_log(self, mode="a+"):
        return open(self.logfilename, mode)

    def watch_log(self, fo, needle):
        while True:
            for line in fo:
                if needle in line:
                    return


@pytest.fixture(scope="session")
def worker():
    manager = WorkerManager()
    manager.start()
    try:
        yield manager
    finally:
        manager.stop()


@pytest.fixture(scope="session")
def restart_worker():
    manager = WorkerManager(name="workers-restart")
    manager.start()
    try:
        yield manager
    finally:
        manager.stop()
