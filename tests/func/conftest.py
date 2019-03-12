from contextlib import contextmanager, closing
from subprocess import Popen
from select import select
from warnings import filterwarnings

import pytest
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


filterwarnings("ignore", message="The psycopg2 wheel package will be renamed")


class Listener(object):
    def __init__(self):
        self.conn = self.cursor = None

    def __enter__(self):
        self.conn = psycopg2.connect("")
        self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        self.cursor = self.conn.cursor()
        self.cursor.execute(f'LISTEN "dramatiq.default.ack";')

    def __exit__(self, *_):
        self.cursor.close()
        self.conn.close()
        self.conn = self.cursor = None

    def wait(self, count=1):
        self.conn.notifies[:] = []
        self.conn.poll()
        while len(self.conn.notifies) < count:
            rlist, *_ = select([self.conn], [], [], 300)
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


@pytest.fixture(scope='session')
def worker():
    logfile = "my-workers.log"
    ready = False
    with open(logfile, "w+") as fo:
        proc = Popen([
            "dramatiq",
            "--verbose", "--log-file", logfile,
            "--processes=1", "--threads=8",
            "example",
        ])

        while not ready:
            for line in fo:
                ready = "Worker process is ready" in line
                if ready:
                    break

    try:
        yield proc
    finally:
        proc.terminate()
        proc.communicate()
