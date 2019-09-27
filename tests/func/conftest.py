import os
import signal
from contextlib import contextmanager, closing
from subprocess import Popen
from select import select
from time import sleep
from warnings import filterwarnings

import pytest
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


filterwarnings("ignore", message="The psycopg2 wheel package will be renamed")


class Listener(object):
    class Timeout(Exception):
        pass

    def __init__(self):
        self.conn = self.cursor = None

    def __enter__(self):
        self.conn = psycopg2.connect("")
        self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        self.cursor = self.conn.cursor()
        self.cursor.execute(f'LISTEN "dramatiq.default.ack";')
        self.notifies = self.conn.notifies  # Useful for debugging.

    def __exit__(self, *_):
        self.cursor.close()
        self.conn.close()
        self.conn = self.cursor = None

    def wait(self, count=1, timeout=30):
        self.conn.notifies[:] = []
        self.conn.poll()
        select_timeout = min(5, timeout)
        while len(self.conn.notifies) < count:
            if timeout <= 0:
                raise self.Timeout("Timeout")
            timeout -= select_timeout
            rlist, *_ = select([self.conn], [], [], select_timeout)
            if not rlist:
                continue  # Loop on timeout
            self.conn.poll()

        return self.conn.notifies.copy()


@contextmanager
def pgconn_manager():
    conn = psycopg2.connect("")
    with closing(conn):
        with conn:
            curs = conn.cursor()
            with closing(curs):
                yield curs


def truncate(table):
    with pgconn_manager() as curs:
        curs.execute(f'TRUNCATE {table};')


@pytest.fixture(autouse=True)
def pgconn():
    return pgconn_manager


@pytest.fixture(scope='session', autouse=True)
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
    def __init__(self, name='workers'):
        self.logfilename = f"my-{name}.log"

    def start(self):
        self.logfo = self.open_log("w+")
        self.proc = Popen([
            "dramatiq",
            "--verbose", "--log-file", self.logfilename,
            "--processes=4", "--threads=2",
            "example",
        ], start_new_session=True)
        self.watch_log(self.logfo, needle="Worker process is ready")

    def stop(self, *_):
        self.proc.poll()
        if self.proc.returncode is None:
            self.proc.terminate()
            sleep(.5)
            self.proc.poll()
        if self.proc.returncode is not None:
            self.proc.terminate()
        self.proc.communicate()

    def crash(self):
        pgid = os.getpgid(self.proc.pid)
        os.killpg(pgid, signal.SIGKILL)

    def open_log(self, mode='a+'):
        return open(self.logfilename, mode)

    def watch_log(self, fo, needle):
        while True:
            for line in fo:
                if needle in line:
                    return


@pytest.fixture(scope='session')
def worker():
    manager = WorkerManager()
    manager.start()
    try:
        yield manager
    finally:
        manager.stop()


@pytest.fixture(scope='session')
def restart_worker():
    manager = WorkerManager(name='workers-restart')
    manager.start()
    try:
        yield manager
    finally:
        manager.stop()
