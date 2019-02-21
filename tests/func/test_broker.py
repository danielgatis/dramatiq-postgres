from random import randint

import pytest

from example import sleeper


@pytest.mark.timeout(8)
def test_massive(listener):
    count = 32
    with listener:
        for _ in range(count):
            sleeper.send(randint(1, 10) / 100)
        listener.wait(count)
