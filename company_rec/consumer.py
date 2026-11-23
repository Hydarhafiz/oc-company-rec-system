"""Run one manual-ack RabbitMQ consumer with database-backed delivery handling."""

from __future__ import annotations

import asyncio
import logging
import signal
from typing import Protocol

import aio_pika

from company_rec.config import DATABASE_URL, FASTEMBED_CACHE_PATH, RABBITMQ_URL
from company_rec.database import CompanyDatabase
from company_rec.embeddings import BusinessEmbedder
from company_rec.events import EventValidationError, parse_event

QUEUE_NAME = "company-events"
PREFETCH_COUNT = 1
MAX_RETRY_DELAY_SECONDS = 30.0
logger = logging.getLogger(__name__)


class Delivery(Protocol):
    body: bytes
    message_id: str | None

    async def ack(self) -> None: ...


async def process_delivery(
    delivery: Delivery,
    database: CompanyDatabase,
    embedder: BusinessEmbedder,
) -> str:
    try:
        event = parse_event(delivery.body, delivery.message_id)
    except EventValidationError as error:
        await database.quarantine(delivery.body, delivery.message_id, error.code, error.detail)
        await delivery.ack()
        logger.info("quarantined delivery: %s", error.code)
        return "quarantined"

    embedding = await embedder.for_company(event)
    changed = await database.upsert_company(event, embedding)
    await delivery.ack()
    return "updated" if changed else "unchanged"


async def consume_session(
    url: str,
    database: CompanyDatabase,
    embedder: BusinessEmbedder,
    stop_event: asyncio.Event,
) -> None:
    connection = await aio_pika.connect(url)
    active_delivery = asyncio.Event()
    active_delivery.set()
    active_callbacks = 0
    callback_failed = asyncio.Event()
    errors: list[Exception] = []
    consumer_tag: str | None = None
    wait_tasks: list[asyncio.Future[bool]] = []

    try:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=PREFETCH_COUNT)
        queue = await channel.declare_queue(QUEUE_NAME, durable=True)

        async def on_delivery(delivery: aio_pika.abc.AbstractIncomingMessage) -> None:
            nonlocal active_callbacks
            active_callbacks += 1
            active_delivery.clear()
            try:
                await process_delivery(delivery, database, embedder)
            except Exception as error:
                errors.append(error)
                logger.exception("delivery failed; leaving it unacknowledged")
                callback_failed.set()
            finally:
                active_callbacks -= 1
                if active_callbacks == 0:
                    active_delivery.set()

        consumer_tag = await queue.consume(on_delivery, no_ack=False)
        stop_task = asyncio.create_task(stop_event.wait())
        failure_task = asyncio.create_task(callback_failed.wait())
        closed_task = asyncio.ensure_future(connection.closed())
        wait_tasks = [stop_task, failure_task, closed_task]
        await asyncio.wait(wait_tasks, return_when=asyncio.FIRST_COMPLETED)

        if not closed_task.done():
            try:
                await queue.cancel(consumer_tag)
            except Exception:
                logger.debug("queue cancellation failed while closing the consumer", exc_info=True)

        await active_delivery.wait()
        if errors:
            raise errors[0]
        if stop_event.is_set():
            return
        raise ConnectionError("RabbitMQ connection closed")
    finally:
        for task in wait_tasks:
            if not task.done():
                task.cancel()
        await connection.close()


def retry_delay(failure_count: int) -> float:
    exponent = min(max(failure_count, 0), 6)
    return min(MAX_RETRY_DELAY_SECONDS, 0.5 * (2**exponent))


async def run_consumer(
    url: str,
    database: CompanyDatabase,
    embedder: BusinessEmbedder,
    stop_event: asyncio.Event,
) -> None:
    failure_count = 0
    while not stop_event.is_set():
        try:
            await consume_session(url, database, embedder, stop_event)
            return
        except asyncio.CancelledError:
            raise
        except Exception:
            failure_count += 1
            delay = retry_delay(failure_count)
            logger.exception("consumer session failed; retrying in %.1f seconds", delay)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=delay)
            except TimeoutError:
                pass


async def run(embedder: BusinessEmbedder) -> None:
    database = await CompanyDatabase.connect(DATABASE_URL)
    try:
        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for shutdown_signal in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(shutdown_signal, stop_event.set)
            except NotImplementedError:
                signal.signal(
                    shutdown_signal,
                    lambda *_: loop.call_soon_threadsafe(stop_event.set),
                )
        await run_consumer(RABBITMQ_URL, database, embedder, stop_event)
    finally:
        await database.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    embedder = BusinessEmbedder.load(FASTEMBED_CACHE_PATH)
    try:
        asyncio.run(run(embedder))
    finally:
        embedder.close()


if __name__ == "__main__":
    main()
