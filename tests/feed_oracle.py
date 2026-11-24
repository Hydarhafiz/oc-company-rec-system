"""Independent interpretation of the original feed for end-to-end evidence."""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

REGISTRATION = re.compile(r"SYN-[0-9]{6}\Z")
MALAYSIA_TIME = ZoneInfo("Asia/Kuala_Lumpur")
GENERIC_TEXT = "Company business activity unspecified."


@dataclass(frozen=True, slots=True)
class ExpectedCompany:
    registration_number: str
    source_updated_at: datetime
    last_message_id: str
    company_name: str | None
    nature_of_business: str | None
    industry: str | None
    state: str | None
    embedding_source: str
    embedding_text: str


@dataclass(frozen=True, slots=True)
class ExpectedFailure:
    message_id: str
    body_sha256: bytes
    raw_body: bytes
    reason_code: str


@dataclass(frozen=True, slots=True)
class FeedOracle:
    deliveries: int
    accepted_deliveries: int
    companies: dict[str, ExpectedCompany]
    failures: dict[tuple[str, bytes], ExpectedFailure]
    failure_counts: Counter[str]


class RejectedFeedLine(Exception):
    def __init__(self, reason_code: str) -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code


def _timestamp(value: object) -> datetime:
    if isinstance(value, bool):
        raise RejectedFeedLine("invalid_updated_at")
    if isinstance(value, int):
        try:
            seconds, milliseconds = divmod(value, 1000)
            return datetime.fromtimestamp(seconds, tz=UTC).replace(microsecond=milliseconds * 1000)
        except (OverflowError, OSError, ValueError) as error:
            raise RejectedFeedLine("invalid_updated_at") from error
    if not isinstance(value, str):
        raise RejectedFeedLine("invalid_updated_at")

    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    if "T" not in normalized and " " not in normalized:
        raise RejectedFeedLine("invalid_updated_at")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as error:
        raise RejectedFeedLine("invalid_updated_at") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=MALAYSIA_TIME)
    return parsed.astimezone(UTC)


def _optional_text(data: dict[str, object], key: str) -> str | None:
    value = data.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise RejectedFeedLine("invalid_company_data")
    return value.strip() or None


def _interpret(body: bytes, message_id: str) -> ExpectedCompany:
    try:
        payload = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise RejectedFeedLine("invalid_json") from error
    if not isinstance(payload, dict):
        raise RejectedFeedLine("invalid_envelope")

    event_type = payload.get("type")
    if not isinstance(event_type, str):
        raise RejectedFeedLine("invalid_event_type")
    if event_type != "company-upserted":
        raise RejectedFeedLine("unsupported_event_type")

    raw_registration = payload.get("registrationNumber")
    if not isinstance(raw_registration, str):
        raise RejectedFeedLine("invalid_registration_number")
    registration_number = raw_registration.strip().upper()
    if REGISTRATION.fullmatch(registration_number) is None:
        raise RejectedFeedLine("invalid_registration_number")

    raw_data = payload.get("data")
    if not isinstance(raw_data, dict):
        raise RejectedFeedLine("invalid_company_data")
    company_name = _optional_text(raw_data, "companyName")
    nature_of_business = _optional_text(raw_data, "natureOfBusiness")
    industry = _optional_text(raw_data, "industry")
    state = _optional_text(raw_data, "state")

    source_updated_at = _timestamp(payload.get("updatedAt"))
    if not message_id:
        raise RejectedFeedLine("missing_message_id")

    if nature_of_business:
        embedding_source, embedding_text = "nature_of_business", nature_of_business
    elif industry:
        embedding_source, embedding_text = "industry", industry
    elif company_name:
        embedding_source, embedding_text = "company_name", company_name
    else:
        embedding_source, embedding_text = "generic", GENERIC_TEXT

    return ExpectedCompany(
        registration_number=registration_number,
        source_updated_at=source_updated_at,
        last_message_id=message_id,
        company_name=company_name,
        nature_of_business=nature_of_business,
        industry=industry,
        state=state,
        embedding_source=embedding_source,
        embedding_text=embedding_text,
    )


def build_feed_oracle(lines: list[str]) -> FeedOracle:
    companies: dict[str, ExpectedCompany] = {}
    failures: dict[tuple[str, bytes], ExpectedFailure] = {}
    failure_counts: Counter[str] = Counter()
    accepted_deliveries = 0

    for line in lines:
        body = line.encode("utf-8")
        message_id = hashlib.sha256(body).hexdigest()
        body_sha256 = hashlib.sha256(body).digest()
        try:
            expected = _interpret(body, message_id)
        except RejectedFeedLine as error:
            failure_counts[error.reason_code] += 1
            failures[(message_id, body_sha256)] = ExpectedFailure(
                message_id=message_id,
                body_sha256=body_sha256,
                raw_body=body,
                reason_code=error.reason_code,
            )
            continue

        accepted_deliveries += 1
        current = companies.get(expected.registration_number)
        incoming_version = (expected.source_updated_at, expected.last_message_id.encode("ascii"))
        current_version = (
            (current.source_updated_at, current.last_message_id.encode("ascii")) if current else None
        )
        if current_version is None or incoming_version > current_version:
            companies[expected.registration_number] = expected

    return FeedOracle(
        deliveries=len(lines),
        accepted_deliveries=accepted_deliveries,
        companies=companies,
        failures=failures,
        failure_counts=failure_counts,
    )


def oracle_from_path(path: Path) -> FeedOracle:
    return build_feed_oracle(path.read_text(encoding="utf-8").splitlines())
