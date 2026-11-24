"""Run isolated RabbitMQ/PostgreSQL failure and redelivery scenarios."""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
import signal
import subprocess
import sys
import time
import urllib.request
from datetime import UTC, datetime

import aio_pika
import asyncpg

QUEUE_NAME = "company-events"
TEST_IDS = ("SYN-999998", "SYN-999997", "SYN-999996")
FAILURE_SEQUENCE = "f3_live_fail_once_seq"
FAILURE_FUNCTION = "f3_live_fail_once"
FAILURE_TRIGGER = "f3_live_fail_once_trigger"
QUARANTINE_SEQUENCE = "f3_live_quarantine_fail_seq"
QUARANTINE_FUNCTION = "f3_live_quarantine_fail"
QUARANTINE_TRIGGER = "f3_live_quarantine_fail_trigger"
INVALID_TEST_BODY = b"{f3-invalid-json"
INVALID_TEST_MESSAGE_ID = hashlib.sha256(INVALID_TEST_BODY).hexdigest()


class RecoveryCheckError(RuntimeError):
    pass


def broker_queue(management_url: str) -> dict[str, object]:
    request = urllib.request.Request(f"{management_url.rstrip('/')}/api/queues/%2F/{QUEUE_NAME}")
    request.add_header("Authorization", "Basic " + base64.b64encode(b"guest:guest").decode())
    with urllib.request.urlopen(request, timeout=8) as response:
        return json.load(response)


def stats(queue: dict[str, object]) -> dict[str, int]:
    values = queue.get("message_stats") or {}
    return {
        key: int(values.get(key, 0))
        for key in ("publish", "deliver", "ack", "redeliver")
    }


async def publish_test_event(url: str, registration_number: str, label: str) -> str:
    payload = {
        "type": "company-upserted",
        "registrationNumber": registration_number,
        "updatedAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "data": {
            "companyName": label,
            "natureOfBusiness": f"{label} isolated recovery scenario",
            "industry": "F3 verification",
            "state": "Local test only",
        },
    }
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return await publish_body(url, body)


async def publish_body(url: str, body: bytes) -> str:
    message_id = hashlib.sha256(body).hexdigest()
    connection = await aio_pika.connect(url)
    try:
        channel = await connection.channel(publisher_confirms=True)
        await channel.declare_queue(QUEUE_NAME, durable=True)
        await channel.default_exchange.publish(
            aio_pika.Message(
                body,
                message_id=message_id,
                content_type="application/json",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key=QUEUE_NAME,
        )
    finally:
        await connection.close()
    return message_id


class RecoveryHarness:
    def __init__(self, dsn: str, broker_url: str, cache_dir: str | None, management_url: str) -> None:
        self.dsn = dsn
        self.broker_url = broker_url
        self.cache_dir = cache_dir
        self.management_url = management_url
        self.database: asyncpg.Connection | None = None
        self.children: list[asyncio.subprocess.Process] = []
        self.published_ids: set[str] = set()
        self.expected_company_message_ids: dict[str, str] = {}
        self.entry_state_confirmed = False
        self.results: dict[str, object] = {}

    async def run(self) -> dict[str, object]:
        self.database = await asyncpg.connect(self.dsn)
        try:
            await self._check_entry_state()
            self.entry_state_confirmed = True
            await self._transaction_failure_redelivery()
            await self._quarantine_failure_redelivery()
            await self._commit_before_ack_redelivery()
            await self._database_unavailable_startup_recovery()
            await self._cleanup()
            return self.results
        finally:
            await self._stop_all_children()
            await self._drop_failure_injection()
            await self._drop_quarantine_injection()
            if self.database is not None:
                await self._wait_until(
                    lambda current: int(current.get("messages_unacknowledged", 0)) == 0
                    and int(current.get("consumers", 0)) == 0,
                    "consumer shutdown before test-message cleanup",
                    timeout=30,
                )
                await self._drain_own_messages()
                if self.entry_state_confirmed:
                    for registration_number, message_id in self.expected_company_message_ids.items():
                        await self.database.execute(
                            "DELETE FROM companies WHERE registration_number = $1 AND last_message_id = $2",
                            registration_number,
                            message_id,
                        )
                    await self.database.execute(
                        "DELETE FROM failed_events WHERE message_id = $1 AND body_sha256 = $2",
                        INVALID_TEST_MESSAGE_ID,
                        hashlib.sha256(INVALID_TEST_BODY).digest(),
                    )
                await self.database.close()

    async def _check_entry_state(self) -> None:
        assert self.database is not None
        counts = await self.database.fetchrow(
            "SELECT (SELECT count(*) FROM companies) AS companies, "
            "(SELECT count(*) FROM failed_events) AS failed_events"
        )
        if counts["companies"] != 2_000 or counts["failed_events"] != 40:
            raise RecoveryCheckError(f"unexpected starting database state: {dict(counts)!r}")
        occupied = await self.database.fetchval(
            "SELECT count(*) FROM companies WHERE registration_number = ANY($1::text[])",
            list(TEST_IDS),
        )
        if occupied:
            raise RecoveryCheckError("one or more isolated F3 test IDs already exist")
        queue = broker_queue(self.management_url)
        if any(int(queue.get(name, 0)) for name in ("messages_ready", "messages_unacknowledged", "consumers")):
            raise RecoveryCheckError(f"company-events must be empty with no consumers: {queue!r}")
        protected_objects = await self.database.fetchrow(
            """SELECT
                   (SELECT count(*) FROM pg_class WHERE relname = ANY($1::text[])) AS relations,
                   (SELECT count(*) FROM pg_proc WHERE proname = ANY($2::text[])) AS functions,
                   (SELECT count(*) FROM pg_trigger WHERE tgname = ANY($3::text[])) AS triggers""",
            [FAILURE_SEQUENCE, QUARANTINE_SEQUENCE],
            [FAILURE_FUNCTION, QUARANTINE_FUNCTION],
            [FAILURE_TRIGGER, QUARANTINE_TRIGGER],
        )
        if any(protected_objects.values()):
            raise RecoveryCheckError(
                f"reserved F3 failure-injection objects already exist: {dict(protected_objects)!r}"
            )
        failure_rows = await self.database.fetchval(
            "SELECT count(*) FROM failed_events WHERE message_id = $1 AND body_sha256 = $2",
            INVALID_TEST_MESSAGE_ID,
            hashlib.sha256(INVALID_TEST_BODY).digest(),
        )
        if failure_rows:
            raise RecoveryCheckError("the isolated F3 quarantine test identity already exists")

    async def _start_worker(self, *, crash_after_commit_for: str | None = None, dsn: str | None = None):
        command = [
            sys.executable,
            "-m",
            "tests.live_consumer",
            "--dsn",
            dsn or self.dsn,
            "--rabbitmq-url",
            self.broker_url,
        ]
        if self.cache_dir:
            command.extend(("--cache-dir", self.cache_dir))
        if crash_after_commit_for:
            command.extend(("--crash-after-commit-for", crash_after_commit_for))
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self.children.append(process)
        if dsn is not None:
            return process

        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if process.returncode is not None:
                raise RecoveryCheckError(f"consumer exited during startup: {process.returncode}")
            queue = broker_queue(self.management_url)
            if int(queue.get("consumers", 0)) == 1:
                return process
            await asyncio.sleep(0.1)
        raise RecoveryCheckError("consumer did not register within 30 seconds")

    async def _stop_worker(self, process: asyncio.subprocess.Process) -> None:
        if process.returncode is None:
            process.send_signal(signal.SIGTERM)
            try:
                await asyncio.wait_for(process.wait(), timeout=15)
            except TimeoutError:
                process.kill()
                await process.wait()
        if process in self.children:
            self.children.remove(process)

    async def _wait_until(self, predicate, description: str, timeout: float = 60.0) -> dict[str, object]:
        deadline = time.monotonic() + timeout
        last: dict[str, object] = {}
        while time.monotonic() < deadline:
            last = broker_queue(self.management_url)
            if predicate(last):
                return last
            await asyncio.sleep(0.1)
        raise RecoveryCheckError(f"timed out waiting for {description}: {last!r}")

    async def _row(self, registration_number: str) -> asyncpg.Record | None:
        assert self.database is not None
        return await self.database.fetchrow(
            """SELECT registration_number, company_name, nature_of_business, industry, state,
                      embedding::text AS embedding, source_updated_at, last_message_id
               FROM companies WHERE registration_number = $1""",
            registration_number,
        )

    async def _transaction_failure_redelivery(self) -> None:
        assert self.database is not None
        await self.database.execute(f"CREATE SEQUENCE public.{FAILURE_SEQUENCE} START WITH 1")
        await self.database.execute(
            f"""CREATE FUNCTION public.{FAILURE_FUNCTION}() RETURNS trigger
                LANGUAGE plpgsql AS $f3$
                BEGIN
                    IF NEW.registration_number = '{TEST_IDS[0]}' THEN
                        IF nextval('public.{FAILURE_SEQUENCE}') = 1 THEN
                            PERFORM pg_sleep(2);
                            RAISE EXCEPTION 'F3 controlled transaction failure';
                        END IF;
                    END IF;
                    RETURN NEW;
                END
                $f3$"""
        )
        await self.database.execute(
            f"""CREATE TRIGGER {FAILURE_TRIGGER} BEFORE INSERT OR UPDATE ON public.companies
                FOR EACH ROW EXECUTE FUNCTION public.{FAILURE_FUNCTION}()"""
        )

        worker = await self._start_worker()
        before = stats(broker_queue(self.management_url))
        message_id = await publish_test_event(self.broker_url, TEST_IDS[0], "F3 Pre-commit Failure")
        self.published_ids.add(message_id)
        self.expected_company_message_ids[TEST_IDS[0]] = message_id
        deadline = time.monotonic() + 60
        final_queue: dict[str, object] = {}
        while time.monotonic() < deadline:
            final_queue = broker_queue(self.management_url)
            current_stats = stats(final_queue)
            attempts = await self.database.fetchval(f"SELECT last_value FROM public.{FAILURE_SEQUENCE}")
            if (
                await self._row(TEST_IDS[0]) is not None
                and int(final_queue.get("messages_ready", 0)) == 0
                and int(final_queue.get("messages_unacknowledged", 0)) == 0
                and current_stats["ack"] == before["ack"] + 1
                and current_stats["deliver"] == before["deliver"] + 2
                and current_stats["redeliver"] == before["redeliver"] + 1
                and attempts == 2
            ):
                break
            await asyncio.sleep(0.1)
        else:
            raise RecoveryCheckError("transaction failure did not recover through broker redelivery")

        row = await self._row(TEST_IDS[0])
        if row is None:
            raise RecoveryCheckError("redelivered transaction did not persist a company row")
        self.results["transaction_failure_redelivery"] = {
            "first_transaction_failed_and_rolled_back": True,
            "broker_delivery_increment": 2,
            "broker_redelivery_increment": 1,
            "ack_increment_after_successful_retry": 1,
            "consumer_restarted_and_redelivery_committed": True,
            "sequence_attempts": attempts,
        }
        await self._stop_worker(worker)
        await self._drop_failure_injection()

    async def _quarantine_failure_redelivery(self) -> None:
        assert self.database is not None
        await self.database.execute(f"CREATE SEQUENCE public.{QUARANTINE_SEQUENCE} START WITH 1")
        await self.database.execute(
            f"""CREATE FUNCTION public.{QUARANTINE_FUNCTION}() RETURNS trigger
                LANGUAGE plpgsql AS $f3$
                BEGIN
                    IF NEW.message_id = '{INVALID_TEST_MESSAGE_ID}' THEN
                        IF nextval('public.{QUARANTINE_SEQUENCE}') = 1 THEN
                            PERFORM pg_sleep(2);
                            RAISE EXCEPTION 'F3 controlled quarantine transaction failure';
                        END IF;
                    END IF;
                    RETURN NEW;
                END
                $f3$"""
        )
        await self.database.execute(
            f"""CREATE TRIGGER {QUARANTINE_TRIGGER} BEFORE INSERT ON public.failed_events
                FOR EACH ROW EXECUTE FUNCTION public.{QUARANTINE_FUNCTION}()"""
        )

        worker = await self._start_worker()
        before = stats(broker_queue(self.management_url))
        self.published_ids.add(await publish_body(self.broker_url, INVALID_TEST_BODY))
        deadline = time.monotonic() + 60
        final_queue: dict[str, object] = {}
        attempts = 0
        while time.monotonic() < deadline:
            final_queue = broker_queue(self.management_url)
            current_stats = stats(final_queue)
            attempts = await self.database.fetchval(f"SELECT last_value FROM public.{QUARANTINE_SEQUENCE}")
            identity_count = await self.database.fetchval(
                "SELECT count(*) FROM failed_events WHERE message_id = $1 AND body_sha256 = $2",
                INVALID_TEST_MESSAGE_ID,
                hashlib.sha256(INVALID_TEST_BODY).digest(),
            )
            if (
                identity_count == 1
                and int(final_queue.get("messages_ready", 0)) == 0
                and int(final_queue.get("messages_unacknowledged", 0)) == 0
                and current_stats["ack"] == before["ack"] + 1
                and current_stats["deliver"] == before["deliver"] + 2
                and current_stats["redeliver"] == before["redeliver"] + 1
                and attempts == 2
            ):
                break
            await asyncio.sleep(0.1)
        else:
            raise RecoveryCheckError("failed quarantine write did not recover through broker redelivery")

        failure = await self.database.fetchrow(
            "SELECT raw_body, reason_code FROM failed_events WHERE message_id = $1 AND body_sha256 = $2",
            INVALID_TEST_MESSAGE_ID,
            hashlib.sha256(INVALID_TEST_BODY).digest(),
        )
        if failure is None or failure["raw_body"] != INVALID_TEST_BODY or failure["reason_code"] != "invalid_json":
            raise RecoveryCheckError("redelivered invalid event was not quarantined with its original bytes")
        self.results["quarantine_failure_redelivery"] = {
            "first_quarantine_transaction_failed": True,
            "quarantine_identity_attempts": attempts,
            "broker_delivery_increment": 2,
            "broker_redelivery_increment": 1,
            "ack_increment_after_durable_quarantine": 1,
            "raw_invalid_body_preserved": True,
        }
        await self._stop_worker(worker)
        await self._drop_quarantine_injection()

    async def _commit_before_ack_redelivery(self) -> None:
        assert self.database is not None
        before = stats(broker_queue(self.management_url))
        crashing_worker = await self._start_worker(crash_after_commit_for=TEST_IDS[1])
        message_id = await publish_test_event(self.broker_url, TEST_IDS[1], "F3 Commit Before Ack")
        self.published_ids.add(message_id)
        self.expected_company_message_ids[TEST_IDS[1]] = message_id
        try:
            exit_code = await asyncio.wait_for(crashing_worker.wait(), timeout=60)
        except TimeoutError as error:
            raise RecoveryCheckError("injected consumer did not exit after its database commit") from error
        if exit_code != 86:
            raise RecoveryCheckError(f"expected injected post-commit exit 86, got {exit_code}")
        self.children.remove(crashing_worker)

        queue = await self._wait_until(
            lambda current: int(current.get("messages_ready", 0)) == 1
            and int(current.get("messages_unacknowledged", 0)) == 0
            and int(current.get("consumers", 0)) == 0
            and stats(current)["ack"] == before["ack"],
            "commit-before-ACK message to be requeued",
        )
        if stats(queue)["ack"] != before["ack"]:
            raise RecoveryCheckError("message was acknowledged before the simulated crash")
        committed_row = await self._row(TEST_IDS[1])
        if committed_row is None:
            raise RecoveryCheckError("database commit did not survive the consumer crash")

        recovery_worker = await self._start_worker()
        final_queue = await self._wait_until(
            lambda current: int(current.get("messages_ready", 0)) == 0
            and int(current.get("messages_unacknowledged", 0)) == 0
            and stats(current)["ack"] == before["ack"] + 1
            and stats(current)["deliver"] == before["deliver"] + 2
            and stats(current)["redeliver"] == before["redeliver"] + 1,
            "commit-before-ACK redelivery to be acknowledged",
        )
        replayed_row = await self._row(TEST_IDS[1])
        if committed_row != replayed_row or stats(final_queue)["redeliver"] != before["redeliver"] + 1:
            raise RecoveryCheckError("commit-before-ACK redelivery changed the committed company snapshot")
        self.results["commit_before_ack_redelivery"] = {
            "consumer_exit_code_after_database_commit": exit_code,
            "committed_row_survived_process_exit": True,
            "broker_redeliver_increment": 1,
            "redelivery_ack_increment": 1,
            "database_snapshot_unchanged_on_replay": True,
        }
        await self._stop_worker(recovery_worker)

    async def _database_unavailable_startup_recovery(self) -> None:
        assert self.database is not None
        before = stats(broker_queue(self.management_url))
        message_id = await publish_test_event(self.broker_url, TEST_IDS[2], "F3 Database Unavailable")
        self.published_ids.add(message_id)
        self.expected_company_message_ids[TEST_IDS[2]] = message_id
        queued = await self._wait_until(
            lambda current: int(current.get("messages_ready", 0)) == 1
            and int(current.get("messages_unacknowledged", 0)) == 0,
            "database-outage test message to remain ready",
        )
        unavailable = await self._start_worker(
            dsn="postgresql://postgres:postgres@127.0.0.1:1/companies"
        )
        try:
            exit_code = await asyncio.wait_for(unavailable.wait(), timeout=20)
        except TimeoutError as error:
            unavailable.kill()
            await unavailable.wait()
            raise RecoveryCheckError("consumer did not report the unavailable database") from error
        self.children.remove(unavailable)
        held_queue = broker_queue(self.management_url)
        if (
            exit_code == 0
            or int(held_queue.get("messages_ready", 0)) != 1
            or int(held_queue.get("messages_unacknowledged", 0)) != 0
            or stats(held_queue)["ack"] != before["ack"]
            or await self._row(TEST_IDS[2]) is not None
        ):
            raise RecoveryCheckError("unavailable-database startup consumed or acknowledged the queued event")

        recovery_worker = await self._start_worker()
        drained = await self._wait_until(
            lambda current: int(current.get("messages_ready", 0)) == 0
            and int(current.get("messages_unacknowledged", 0)) == 0
            and stats(current)["ack"] == before["ack"] + 1,
            "queued event to process after database connectivity was restored",
        )
        if await self._row(TEST_IDS[2]) is None or stats(drained)["redeliver"] != before["redeliver"]:
            raise RecoveryCheckError("database-unavailable startup recovery did not persist the queued event")
        self.results["database_unavailable_startup_recovery"] = {
            "unavailable_database_exit_code": exit_code,
            "message_remained_ready_with_zero_unacknowledged": True,
            "healthy_restart_persisted_and_acked_event": True,
            "redelivery_increment": 0,
            "scope": "startup connection failure; active PostgreSQL service interruption was not induced",
        }
        await self._stop_worker(recovery_worker)

    async def _drop_failure_injection(self) -> None:
        if self.database is None or self.database.is_closed():
            return
        await self.database.execute(f"DROP TRIGGER IF EXISTS {FAILURE_TRIGGER} ON public.companies")
        await self.database.execute(f"DROP FUNCTION IF EXISTS public.{FAILURE_FUNCTION}()")
        await self.database.execute(f"DROP SEQUENCE IF EXISTS public.{FAILURE_SEQUENCE}")

    async def _drop_quarantine_injection(self) -> None:
        if self.database is None or self.database.is_closed():
            return
        await self.database.execute(f"DROP TRIGGER IF EXISTS {QUARANTINE_TRIGGER} ON public.failed_events")
        await self.database.execute(f"DROP FUNCTION IF EXISTS public.{QUARANTINE_FUNCTION}()")
        await self.database.execute(f"DROP SEQUENCE IF EXISTS public.{QUARANTINE_SEQUENCE}")

    async def _drain_own_messages(self) -> None:
        if not self.published_ids:
            return
        connection = await aio_pika.connect(self.broker_url)
        try:
            channel = await connection.channel()
            queue = await channel.declare_queue(QUEUE_NAME, durable=True)
            while True:
                message = await queue.get(no_ack=False, fail=False)
                if message is None:
                    return
                if message.message_id not in self.published_ids:
                    await message.nack(requeue=True)
                    raise RecoveryCheckError(
                        "recovery cleanup encountered an unknown queue message; it was requeued"
                    )
                await message.ack()
        finally:
            await connection.close()

    async def _cleanup(self) -> None:
        assert self.database is not None
        deleted_statuses = []
        for registration_number, message_id in self.expected_company_message_ids.items():
            deleted_statuses.append(
                await self.database.execute(
                    "DELETE FROM companies WHERE registration_number = $1 AND last_message_id = $2",
                    registration_number,
                    message_id,
                )
            )
        deleted_failure = await self.database.execute(
            "DELETE FROM failed_events WHERE message_id = $1 AND body_sha256 = $2",
            INVALID_TEST_MESSAGE_ID,
            hashlib.sha256(INVALID_TEST_BODY).digest(),
        )
        remaining = 0
        for registration_number in self.expected_company_message_ids:
            remaining += await self.database.fetchval(
                "SELECT count(*) FROM companies WHERE registration_number = $1",
                registration_number,
            )
        if remaining:
            raise RecoveryCheckError(f"test company cleanup left {remaining} records")
        remaining_failures = await self.database.fetchval(
            "SELECT count(*) FROM failed_events WHERE message_id = $1 AND body_sha256 = $2",
            INVALID_TEST_MESSAGE_ID,
            hashlib.sha256(INVALID_TEST_BODY).digest(),
        )
        if remaining_failures:
            raise RecoveryCheckError("test quarantine cleanup left a failure identity")
        final_counts = await self.database.fetchrow(
            "SELECT (SELECT count(*) FROM companies) AS companies, "
            "(SELECT count(*) FROM failed_events) AS failed_events"
        )
        if final_counts["companies"] != 2_000 or final_counts["failed_events"] != 40:
            raise RecoveryCheckError(f"recovery cleanup changed the baseline row counts: {dict(final_counts)!r}")
        self.results["cleanup"] = {
            "test_company_ids_removed": list(TEST_IDS),
            "test_company_ids_remaining": remaining,
            "delete_command_statuses": deleted_statuses,
            "test_quarantine_identity_delete_status": deleted_failure,
            "test_quarantine_identity_remaining": remaining_failures,
            "baseline_company_count_restored": final_counts["companies"],
            "baseline_failure_identity_count_restored": final_counts["failed_events"],
        }
        queue = await self._wait_until(
            lambda current: all(
                int(current.get(name, 0)) == 0
                for name in ("messages_ready", "messages_unacknowledged", "consumers")
            ),
            "recovery test queue cleanup",
            timeout=30,
        )
        self.results["final_queue_state"] = {
            "messages_ready": int(queue.get("messages_ready", 0)),
            "messages_unacknowledged": int(queue.get("messages_unacknowledged", 0)),
            "consumers": int(queue.get("consumers", 0)),
            "broker_totals": stats(queue),
        }

    async def _stop_all_children(self) -> None:
        for child in list(self.children):
            await self._stop_worker(child)


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", default="postgresql://postgres:postgres@localhost:5432/companies")
    parser.add_argument("--rabbitmq-url", default="amqp://guest:guest@localhost:5672/")
    parser.add_argument("--management-url", default="http://127.0.0.1:15672")
    parser.add_argument("--cache-dir", help="override FASTEMBED_CACHE_PATH for the child consumers")
    args = parser.parse_args()
    result = await RecoveryHarness(args.dsn, args.rabbitmq_url, args.cache_dir, args.management_url).run()
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    asyncio.run(main())
