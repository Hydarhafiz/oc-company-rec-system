import asyncio
from datetime import UTC, datetime

import pytest

from company_rec.consumer import consume_session, process_delivery, retry_delay, run_consumer
from company_rec.embeddings import BusinessEmbedder
from tests.helpers import embedding, feed_body


class FakeStore:
    def __init__(self):
        self.actions = []
        self.company_versions = {}
        self.failure = None

    async def upsert_company(self, event, generated):
        if self.failure:
            raise self.failure
        self.actions.append(("upsert", event.registration_number))
        version = (event.source_updated_at, event.message_id)
        previous = self.company_versions.get(event.registration_number)
        if previous is not None and version <= previous:
            return False
        self.company_versions[event.registration_number] = version
        return True

    async def quarantine(self, raw_body, message_id, reason_code, detail):
        if self.failure:
            raise self.failure
        self.actions.append(("quarantine", message_id, reason_code, raw_body))


class FakeEmbedder:
    async def for_company(self, event):
        return embedding()


class FakeDelivery:
    def __init__(self, body, message_id="message-a", on_ack=None, ack_error=None):
        self.body = body
        self.message_id = message_id
        self.on_ack = on_ack
        self.ack_error = ack_error
        self.ack_attempts = 0

    async def ack(self):
        self.ack_attempts += 1
        if self.on_ack:
            self.on_ack()
        if self.ack_error:
            error, self.ack_error = self.ack_error, None
            raise error


@pytest.mark.asyncio
async def test_ack_happens_only_after_company_transaction_returns():
    store = FakeStore()
    delivery = FakeDelivery(feed_body())
    delivery.on_ack = lambda: store.actions.append(("ack", delivery.ack_attempts))

    outcome = await process_delivery(delivery, store, FakeEmbedder())

    assert outcome == "updated"
    assert store.actions[-2:] == [("upsert", "SYN-000123"), ("ack", 1)]


@pytest.mark.asyncio
async def test_commit_failure_never_acks_the_delivery():
    store = FakeStore()
    store.failure = RuntimeError("database commit failed")
    delivery = FakeDelivery(feed_body())

    with pytest.raises(RuntimeError, match="commit failed"):
        await process_delivery(delivery, store, FakeEmbedder())

    assert delivery.ack_attempts == 0


@pytest.mark.asyncio
async def test_invalid_delivery_is_quarantined_before_ack():
    store = FakeStore()
    delivery = FakeDelivery(b"{bad")
    delivery.on_ack = lambda: store.actions.append(("ack", len(store.actions)))

    outcome = await process_delivery(delivery, store, FakeEmbedder())

    assert outcome == "quarantined"
    assert store.actions[0][0:3] == ("quarantine", "message-a", "invalid_json")
    assert store.actions[0][3] == b"{bad"
    assert store.actions[-1][0] == "ack"


@pytest.mark.asyncio
async def test_quarantine_failure_never_acks_invalid_message():
    store = FakeStore()
    store.failure = RuntimeError("quarantine storage unavailable")
    delivery = FakeDelivery(b"{bad")

    with pytest.raises(RuntimeError, match="quarantine storage"):
        await process_delivery(delivery, store, FakeEmbedder())

    assert delivery.ack_attempts == 0


@pytest.mark.asyncio
async def test_redelivery_after_commit_before_ack_is_an_idempotent_noop():
    store = FakeStore()
    first_delivery = FakeDelivery(feed_body(), ack_error=ConnectionError("ack lost"))

    with pytest.raises(ConnectionError, match="ack lost"):
        await process_delivery(first_delivery, store, FakeEmbedder())
    second_delivery = FakeDelivery(feed_body())
    outcome = await process_delivery(second_delivery, store, FakeEmbedder())

    assert outcome == "unchanged"
    assert len(store.company_versions) == 1
    assert first_delivery.ack_attempts == second_delivery.ack_attempts == 1


@pytest.mark.asyncio
async def test_duplicate_or_stale_company_write_is_still_acknowledged():
    store = FakeStore()
    store.company_versions["SYN-000123"] = (
        datetime(2026, 8, 14, 1, tzinfo=UTC),
        "message-z",
    )
    delivery = FakeDelivery(feed_body(), message_id="message-a")

    outcome = await process_delivery(delivery, store, FakeEmbedder())

    assert outcome == "unchanged"
    assert delivery.ack_attempts == 1


def test_retry_delay_grows_and_stays_bounded():
    delays = [retry_delay(attempt) for attempt in range(1, 20)]

    assert delays == sorted(delays)
    assert max(delays) == 30.0


@pytest.mark.asyncio
async def test_consumer_retries_a_failed_session_after_backoff(monkeypatch):
    stop = asyncio.Event()
    attempts = 0

    async def fail_then_stop(url, database, embedder, stop_event):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ConnectionError("database is unavailable")
        stop_event.set()

    monkeypatch.setattr("company_rec.consumer.consume_session", fail_then_stop)
    monkeypatch.setattr("company_rec.consumer.retry_delay", lambda _: 0.0)

    await run_consumer("amqp://test", FakeStore(), FakeEmbedder(), stop)

    assert attempts == 2


class FakeQueue:
    def __init__(self):
        self.callback = None
        self.ready = asyncio.Event()
        self.cancelled = False

    async def consume(self, callback, no_ack):
        self.callback = callback
        self.ready.set()
        return "test-consumer"

    async def cancel(self, consumer_tag):
        self.cancelled = True


class FakeChannel:
    def __init__(self, queue):
        self.queue = queue
        self.prefetch_count = None
        self.declared = None

    async def set_qos(self, prefetch_count):
        self.prefetch_count = prefetch_count

    async def declare_queue(self, name, durable):
        self.declared = (name, durable)
        return self.queue


class FakeConnection:
    def __init__(self, queue):
        self.channel_value = FakeChannel(queue)
        self.closed_future = asyncio.get_running_loop().create_future()
        self.close_calls = 0

    async def channel(self):
        return self.channel_value

    def closed(self):
        return asyncio.shield(self.closed_future)

    async def close(self):
        self.close_calls += 1
        if not self.closed_future.done():
            self.closed_future.set_result(True)


@pytest.mark.asyncio
async def test_failed_consumer_callback_closes_connection_with_delivery_unacked(monkeypatch):
    store = FakeStore()
    store.failure = RuntimeError("database unavailable")
    queue = FakeQueue()
    connection = FakeConnection(queue)

    async def connect(url):
        return connection

    monkeypatch.setattr("company_rec.consumer.aio_pika.connect", connect)
    stop = asyncio.Event()
    session = asyncio.create_task(consume_session("amqp://test", store, FakeEmbedder(), stop))
    await queue.ready.wait()
    delivery = FakeDelivery(feed_body())
    await queue.callback(delivery)

    with pytest.raises(RuntimeError, match="database unavailable"):
        await session

    assert queue.cancelled
    assert connection.close_calls == 1
    assert connection.channel_value.prefetch_count == 1
    assert connection.channel_value.declared == ("company-events", True)
    assert delivery.ack_attempts == 0
