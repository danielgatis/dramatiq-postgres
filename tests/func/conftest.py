from subprocess import Popen
from select import select
from time import sleep
from warnings import filterwarnings

import pytest
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


filterwarnings("ignore", message="The psycopg2 wheel package will be renamed")


def truncate_queue_table():
    conn = psycopg2.connect("")
    with conn:
        with conn.cursor() as curs:
            curs.execute("TRUNCATE dramatiq.queue;")
    conn.close()


@pytest.fixture(scope='session', autouse=True)
def flush_queue():
    truncate_queue_table()
    yield None
    truncate_queue_table()


class Listener(object):
    def __init__(self):
        self.conn = None

    def __enter__(self):
        self.conn = psycopg2.connect("")
        self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        self.cursor = self.conn.cursor()
        self.cursor.execute(f'LISTEN "dramatiq.default.ack";')

    def __exit__(self, *_):
        # self.cursor.close()
        # self.conn.close()
        pass

    def wait(self, count):
        self.conn.poll()
        while len(self.conn.notifies) < count:
            rlist, *_ = select([self.conn], [], [], 300)
            if not rlist:
                continue  # Loop on timeout
            self.conn.poll()
        return self.conn.notifies


@pytest.fixture()
def listener():
    return Listener()


@pytest.fixture(scope='session', autouse=True)
def worker():
    logfile = "my-workers.log"
    open(logfile, "w").close()
    proc = Popen([
        "dramatiq",
        "--verbose", "--log-file", logfile,
        "--processes=1", "--threads=8",
        "example",
    ])
    # Wait for workers to listen.
    sleep(1)
    try:
        yield proc
    finally:
        proc.terminate()
        sleep(.25)
        proc.terminate()
        sleep(.25)
        proc.kill()
        proc.communicate()
