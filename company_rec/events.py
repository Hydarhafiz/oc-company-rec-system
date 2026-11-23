"""Validate the registry feed and normalize one complete company snapshot."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, ValidationError, field_validator

MALAYSIA_TIME = ZoneInfo("Asia/Kuala_Lumpur")
REGISTRATION_PATTERN = re.compile(r"SYN-[0-9]{6}\Z")


class EventValidationError(ValueError):
    """A delivery that is malformed or outside the supported event contract."""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail


class CompanyData(BaseModel):
    """Typed subset of the producer's nested data object."""

    model_config = ConfigDict(extra="ignore", strict=True, frozen=True)

    companyName: str | None = None
    natureOfBusiness: str | None = None
    industry: str | None = None
    state: str | None = None

    @field_validator("companyName", "natureOfBusiness", "industry", "state", mode="before")
    @classmethod
    def trim_optional_text(cls, value: Any) -> Any:
        if value is None:
            return None
        if not isinstance(value, str):
            return value
        value = value.strip()
        return value or None


@dataclass(frozen=True, slots=True)
class CompanyEvent:
    registration_number: str
    source_updated_at: datetime
    message_id: str
    company_name: str | None
    nature_of_business: str | None
    industry: str | None
    state: str | None

    @property
    def display_name(self) -> str:
        return self.company_name or self.registration_number


def _parse_updated_at(value: Any) -> datetime:
    if isinstance(value, bool):
        raise ValueError("updatedAt must be an ISO datetime or integer epoch milliseconds")

    if isinstance(value, int):
        try:
            seconds, milliseconds = divmod(value, 1000)
            return datetime.fromtimestamp(seconds, tz=UTC).replace(microsecond=milliseconds * 1000)
        except (OverflowError, OSError, ValueError) as exc:
            raise ValueError("updatedAt epoch milliseconds are out of range") from exc

    if not isinstance(value, str):
        raise ValueError("updatedAt must be an ISO datetime or integer epoch milliseconds")

    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    if "T" not in normalized and " " not in normalized:
        raise ValueError("updatedAt must include a time of day")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError("updatedAt is not a supported ISO datetime") from exc

    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=MALAYSIA_TIME)
    return parsed.astimezone(UTC)


def parse_event(raw_body: bytes, message_id: str | None) -> CompanyEvent:
    """Validate one AMQP delivery, preserving the approved failure ordering."""
    try:
        payload = json.loads(raw_body)
    except (json.JSONDecodeError, UnicodeDecodeError, TypeError) as exc:
        raise EventValidationError("invalid_json", "message body is not valid JSON") from exc

    if not isinstance(payload, dict):
        raise EventValidationError("invalid_envelope", "message body must be a JSON object")

    event_type = payload.get("type")
    if not isinstance(event_type, str):
        raise EventValidationError("invalid_event_type", "event type must be a string")
    if event_type != "company-upserted":
        raise EventValidationError("unsupported_event_type", "event type is not supported")

    raw_registration = payload.get("registrationNumber")
    if not isinstance(raw_registration, str):
        raise EventValidationError("invalid_registration_number", "registrationNumber must be a string")
    registration_number = raw_registration.strip().upper()
    if not REGISTRATION_PATTERN.fullmatch(registration_number):
        raise EventValidationError("invalid_registration_number", "registrationNumber has an unsupported format")

    raw_data = payload.get("data")
    if not isinstance(raw_data, dict):
        raise EventValidationError("invalid_company_data", "data must be a JSON object")

    try:
        data = CompanyData.model_validate(raw_data)
    except ValidationError as exc:
        first_error = exc.errors()[0]
        location = first_error.get("loc", ("data",))
        field = ".".join(str(part) for part in location) or "data"
        raise EventValidationError("invalid_company_data", f"data field {field} must be a string or null") from exc

    try:
        source_updated_at = _parse_updated_at(payload.get("updatedAt"))
    except ValueError as exc:
        raise EventValidationError("invalid_updated_at", str(exc)) from exc

    if not isinstance(message_id, str) or not message_id.strip():
        raise EventValidationError("missing_message_id", "AMQP message_id must be a nonempty string")

    return CompanyEvent(
        registration_number=registration_number,
        source_updated_at=source_updated_at,
        message_id=message_id,
        company_name=data.companyName,
        nature_of_business=data.natureOfBusiness,
        industry=data.industry,
        state=data.state,
    )
