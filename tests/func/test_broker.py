from random import randint

import pytest

from example import (
    failing,
    rejecting,
    writer,
)


@pytest.mark.timeout(8)
def test_massive(listener, pgconn, witness, worker):
    count = 64

    # Start listening for ack.
    with listener:
        # Then queue <count> random messages.
        for n in range(count):
            writer.send(
                randint(1, 10),
                message="Message #%d" % (n,),
            )

        # Wait for *count* ack from workers.
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
