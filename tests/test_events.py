import hashlib
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import pytest

from company_rec.events import EventValidationError, parse_event
from tests.helpers import feed_body

FEED_PATH = Path(__file__).parents[1] / "data" / "feed.jsonl"


@pytest.mark.parametrize(
    ("updated_at", "expected"),
    [
        ("2026-08-14T09:00:00+08:00", datetime(2026, 8, 14, 1, 0, tzinfo=UTC)),
        ("2026-08-14T01:00:00Z", datetime(2026, 8, 14, 1, 0, tzinfo=UTC)),
        ("2026-08-14T09:00:00", datetime(2026, 8, 14, 1, 0, tzinfo=UTC)),
        (123, datetime(1970, 1, 1, 0, 0, 0, 123000, tzinfo=UTC)),
    ],
)
def test_supported_timestamps_normalize_to_utc(updated_at, expected):
    parsed = parse_event(feed_body(updated_at=updated_at), "message-a")

    assert parsed.source_updated_at == expected


def test_registration_and_metadata_are_normalized_without_rewriting_display_case():
    parsed = parse_event(
        feed_body(
            registration_number="  syn-000123  ",
            data={
                "companyName": "  Sinar Teguh Bakery  ",
                "natureOfBusiness": "  ",
                "industry": None,
                "state": " Selangor ",
                "futureField": "ignored",
            },
            source="registry",
            ingestedBy="producer",
        ),
        "message-a",
    )

    assert parsed.registration_number == "SYN-000123"
    assert parsed.company_name == "Sinar Teguh Bakery"
    assert parsed.display_name == "Sinar Teguh Bakery"
    assert parsed.nature_of_business is None
    assert parsed.industry is None
    assert parsed.state == "Selangor"


@pytest.mark.parametrize(
    ("body", "message_id", "expected_code"),
    [
        (b"{bad", "message-a", "invalid_json"),
        (b"null", "message-a", "invalid_envelope"),
        (json.dumps({"registrationNumber": "SYN-000123", "updatedAt": "2026-08-14T09:00:00", "data": {}}).encode(), "message-a", "invalid_event_type"),
        (feed_body(event_type=None), "message-a", "invalid_event_type"),
        (feed_body(event_type="company-struck-off"), "message-a", "unsupported_event_type"),
        (feed_body(registration_number=None), "message-a", "invalid_registration_number"),
        (feed_body(registration_number=123), "message-a", "invalid_registration_number"),
        (feed_body(registration_number="SYN-12-3456"), "message-a", "invalid_registration_number"),
        (feed_body(data=None), "message-a", "invalid_company_data"),
        (feed_body(data="not-an-object"), "message-a", "invalid_company_data"),
        (feed_body(data={"industry": 7}), "message-a", "invalid_company_data"),
        (feed_body(updated_at=None), "message-a", "invalid_updated_at"),
        (feed_body(updated_at=True), "message-a", "invalid_updated_at"),
        (feed_body(updated_at="2026-08-14"), "message-a", "invalid_updated_at"),
        (feed_body(), None, "missing_message_id"),
        (feed_body(), "  ", "missing_message_id"),
    ],
)
def test_invalid_or_unsupported_deliveries_have_stable_failure_codes(body, message_id, expected_code):
    with pytest.raises(EventValidationError) as raised:
        parse_event(body, message_id)

    assert raised.value.code == expected_code


def test_company_name_falls_back_to_normalized_registration_for_display():
    parsed = parse_event(feed_body(registration_number="syn-000123", data={}), "message-a")

    assert parsed.company_name is None
    assert parsed.display_name == "SYN-000123"


def test_real_feed_events_follow_the_validation_contract():
    accepted = []
    failures = Counter()
    failure_identities = set()

    for line in FEED_PATH.read_text(encoding="utf-8").splitlines():
        body = line.encode("utf-8")
        message_id = hashlib.sha256(body).hexdigest()
        try:
            accepted.append(parse_event(body, message_id))
        except EventValidationError as error:
            failures[error.code] += 1
            failure_identities.add((message_id, hashlib.sha256(body).digest()))

    assert len(accepted) == 2_473
    assert len({event.registration_number for event in accepted}) == 2_000
    assert failures == {
        "invalid_json": 5,
        "invalid_envelope": 6,
        "unsupported_event_type": 8,
        "invalid_registration_number": 10,
        "invalid_company_data": 9,
        "invalid_updated_at": 7,
    }
    assert len(failure_identities) == 40
