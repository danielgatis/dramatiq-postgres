import threading


def test_notifier_subscriptions(mocker):
    from dramatiq_postgres.broker import Notifier

    notifier = Notifier(pool=mocker.Mock())
    start_mock = mocker.patch.object(notifier, "start")

    event = threading.Event()
    notifier.subscribe("dramatiq.default.enqueue", event)

    assert start_mock.called
    assert "dramatiq.default.enqueue" in notifier.subscriptions
    assert notifier.dirty.is_set()

    # Subscribing again must not restart the thread.
    other = threading.Event()
    notifier.subscribe("dramatiq.default.enqueue", other)
    assert 1 == start_mock.call_count

    notifier.unsubscribe("dramatiq.default.enqueue", event)
    assert "dramatiq.default.enqueue" in notifier.subscriptions
    notifier.unsubscribe("dramatiq.default.enqueue", other)
    assert "dramatiq.default.enqueue" not in notifier.subscriptions

    # Unsubscribing an unknown channel is a no-op.
    notifier.unsubscribe("unknown", event)
