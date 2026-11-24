from pathlib import Path

from tests.feed_oracle import oracle_from_path

FEED_PATH = Path(__file__).parents[1] / "data" / "feed.jsonl"


def test_independent_feed_oracle_matches_approved_profile_and_winners():
    oracle = oracle_from_path(FEED_PATH)

    assert oracle.deliveries == 2_518
    assert oracle.accepted_deliveries == 2_473
    assert len(oracle.companies) == 2_000
    assert sum(oracle.failure_counts.values()) == 45
    assert len(oracle.failures) == 40
    assert oracle.failure_counts == {
        "invalid_company_data": 9,
        "invalid_envelope": 6,
        "invalid_json": 5,
        "invalid_registration_number": 10,
        "invalid_updated_at": 7,
        "unsupported_event_type": 8,
    }
    assert all(company.source_updated_at.tzinfo is not None for company in oracle.companies.values())
    assert all(company.embedding_text for company in oracle.companies.values())
