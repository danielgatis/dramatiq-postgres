import pytest
from dramatiq import DramatiqError

from example import saver


@pytest.mark.timeout(8)
def test_block(worker):
    message = saver.send(wait=.5, message='test_block message.')

    # There is a race condition here between the wait= and the get_result call.
    # We want to call get_result before saver returns.
    with pytest.raises(DramatiqError):
        result = message.get_result()

    result = message.get_result(block=True)
    assert 'message' in result

    result = message.get_result()
    assert 'message' in result


@pytest.mark.timeout(8)
def test_timeout(worker):
    message = saver.send(wait=.5, message='test_timeout message.')

    with pytest.raises(DramatiqError):
        message.get_result(block=True, timeout=100)

    result = message.get_result(block=True)
    assert 'message' in result
