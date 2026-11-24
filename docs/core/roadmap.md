# Company recommendation system roadmap

Status: **M1 complete** for assessment version **2026.11**. M1 was the only approved milestone; no follow-on milestone is approved. The full contract and feature evidence remain in the [completed milestone](milestone.md), with source authority and feed observations in [planning sources](planning-sources.md).

## Completed milestone: reliable ingestion and recommendation submission

The implementation preserves the supplied producer, feed, and durable `company-events` queue. The Python consumer validates event snapshots, generates cached CPU FastEmbed vectors, writes current company state or quarantine records to PostgreSQL/pgvector, and acknowledges only after the transaction commits. FastAPI serves up to four recommendations using exact cosine distance over stored vectors. See `DESIGN.md` for structure, decisions, run instructions, and limitations.

### Verified outcome

- The unchanged feed's **2,518 deliveries** produced **2,000 company rows** and **40 quarantine identities** (45 invalid or unsupported deliveries). An independent oracle matched every winning source version and metadata field, embedding input/model, and raw quarantine body. All vectors are finite, nonzero, and 384-dimensional.
- A second full replay left a digest of all company columns/vectors and quarantine records unchanged. The live RabbitMQ recovery harness verified company transaction retry, quarantine transaction retry, redelivery after process exit between commit and ACK, and recovery after database-unavailable consumer startup. An in-flight PostgreSQL container outage was not exercised.
- Five live API source IDs each returned four recommendations with source exclusion and database-consistent metadata/order. Invalid input returned `422`, an unknown company `404`, and a controlled unavailable-store instance `503`. Local warmed loopback latency over 100 sequential requests was p50 **2.996 ms**, p95 **3.707 ms**. Full-feed publish-and-drain was **37.656 seconds** under the documented warm-consumer/model conditions.
- Final automated suite: **66 passed, 0 skipped**. README and `DESIGN.md` contain setup, verification, measured evidence, trade-offs, and remaining risks.
- Optional manual review covered two judged source companies and eight recommendations: strict Precision@4 **7/8 = 0.875**. The fallback company was left unscored for insufficient activity metadata. This small sample is not representative; exact-description matches and user-judged cross-language semantic matches are distinguished in `DESIGN.md`.

Assessment time was reported as **120 minutes** before F3 closeout. The final approximate cumulative time and remaining budget are recorded in `DESIGN.md`; the six-hour assessment limit was not exceeded. Stop at M1 for developer review. Do not infer approval for another milestone, publication, or submission.
