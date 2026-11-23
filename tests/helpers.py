"""Small event and vector builders shared by F1 capability tests."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from company_rec.embeddings import GeneratedEmbedding
from company_rec.events import CompanyEvent, parse_event

_UNSET = object()


def feed_body(
    *,
    registration_number: Any = "SYN-000123",
    updated_at: Any = "2026-08-14T09:00:00+08:00",
    data: Any = _UNSET,
    event_type: Any = "company-upserted",
    **extra: Any,
) -> bytes:
    if data is _UNSET:
        data = {
            "companyName": "Sinar Teguh Bakery Sdn. Bhd.",
            "natureOfBusiness": "Operating cafes and coffee shops",
            "industry": "Food & Beverage",
            "state": "Selangor",
        }
    payload = {
        "type": event_type,
        "registrationNumber": registration_number,
        "updatedAt": updated_at,
        "data": data,
        **extra,
    }
    return json.dumps(payload).encode("utf-8")


def event(
    *,
    registration_number: str = "SYN-000123",
    updated_at: str | int = "2026-08-14T09:00:00+08:00",
    message_id: str = "message-a",
    data: dict[str, Any] | object = _UNSET,
) -> CompanyEvent:
    return parse_event(
        feed_body(
            registration_number=registration_number,
            updated_at=updated_at,
            data=data,
        ),
        message_id,
    )


def embedding(marker: float = 1.0, *, source: str = "nature_of_business", text: str = "business") -> GeneratedEmbedding:
    values = [0.0] * 384
    values[0] = marker
    values[1] = 1.0
    return GeneratedEmbedding(source=source, text=text, model="test-model", values=tuple(values))


def utc(year: int, month: int, day: int, hour: int, minute: int = 0) -> datetime:
    return datetime(year, month, day, hour, minute, tzinfo=UTC)
