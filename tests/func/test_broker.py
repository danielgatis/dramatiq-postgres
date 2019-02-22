from random import randint

import pytest

from example import (
    failing,
    rejecting,
    writer,
)


PREFILL = 8


def test_prefill_queue():
    for i in range(PREFILL):
        writer.send(message='prefill', index=i)


@pytest.mark.timeout(4)
def test_process_pending_messages(listener, pgconn, witness, worker):
    # This test must be the first with worker fixture, which starts dramatiq
    # worker process.

    # Actually, there is a race condition between listener.__enter__ which
    # start LISTEN-ing for ack and worker process to start consuming.
    with listener:
        listener.wait(PREFILL)

    # Ensure the witness table has effectively been updated.
    with pgconn() as curs:
        curs.execute("SELECT count(*) FROM functest.witness;")
        count, = curs.fetchone()
    assert PREFILL == count


@pytest.mark.timeout(8)
def test_massive(listener, pgconn, witness):
    count = 64

    # Start listening for ack.
    with listener:
        # Then queue <count> random messages.
        for _ in range(count):
            writer.send(
                randint(1, 10),
                named=randint(1, 10),
            )

        # Wait for 32 ack from workers.
        listener.wait(count)

    # Ensure the witness table has effectively been updated.
    with pgconn() as curs:
        curs.execute("SELECT count(*) FROM functest.witness;")
        witness_count, = curs.fetchone()
    assert count == witness_count


@pytest.mark.timeout(8)
def test_retry(listener, pgconn, witness):
    # Start listening for ack.
    with listener:
        failing.send(always=False, message="Testing retry")

        failed = True
        while failed:
            # Wait for ack of current try.
            listener.wait()

            # Check whether the task has been sucessful.
            with pgconn() as curs:
                curs.execute("SELECT * FROM functest.witness;")
            failed = 0 == curs.rowcount


@pytest.mark.timeout(4)
def test_nack(listener, pgconn, witness):
    with listener:
        rejecting.send(message="Rejecting from func test.")
        listener.wait(1)

    with pgconn() as curs:
        curs.execute("SELECT payload FROM functest.witness LIMIT 1;")
        payload, = curs.fetchone()
    assert 'Rejecting from func test.' == payload['kwargs']['message']
