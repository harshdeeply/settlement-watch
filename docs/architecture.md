# Architecture and operational contract

## Customer problem

A marketplace records captures and refunds in its own ledger while a payment processor sends an independent event stream. Operations needs to know which payment IDs disagree before a settlement deadline. Streams arrive out of order, replay messages, and occasionally deliver records after an export was declared complete. An immediate `missing processor capture` alert can therefore be false.

Settlement Watch treats the internal ledger and processor export as separate sources. It never moves money. An exception is an investigation task, not a fraud finding, payout instruction, or proof of processor error.

## Event path

1. Validate tenant, payment, immutable event ID, source, kind, positive integer minor units, currency, and timezone-aware event time.
2. Within one SQLite transaction, reject a conflicting ID or currency, store the canonical event, and update a payment aggregate. Exact replays do not increment money or revision.
3. Track independent per-tenant source watermarks. A watermark asserts that the source is complete through an event time; it is monotonic. This assertion is trusted but not independently proven.
4. A payment is `BALANCED` when recorded capture and refund totals match. A mismatch stays `WAITING_FOR_SOURCE` until **both** source watermarks reach the latest event time plus a configurable grace period. It then becomes `EXCEPTION`.
5. If a late event arrives after a watermark, it is recorded and increments the payment revision. A former exception can become balanced. Operators must verify the source completeness guarantee and use the revision to avoid acting on an old view.
6. The optional model receives a bounded evidence bundle only for mature exceptions. It may draft a hypothesis and next checks; it cannot set financial totals or exception status. A reviewer owns the decision.

The data structures are keyed by tenant and payment. Event IDs are unique within a tenant, and an indexed event ledger makes per-payment drilldown bounded. The exact count query scans payment aggregates, intentionally outside the ingest path. A single SQLite writer and one-process ingestion do **not** provide multi-node scalability; a production design would partition by `(tenant_id, payment_id)` in a durable stream, persist checkpoints, and materialize status/count views in an analytical store. Watermark metadata needs a source-specific completeness SLA and backfill policy.

## Failure and trust boundaries

- If the processor watermark is delayed, the system waits instead of claiming a missing capture. Alerts can be late. A completeness promise can also be wrong; a subsequent late correction remains possible.
- If the same event ID reappears with a different payload, the batch fails atomically. A real ingestion connector must quarantine, alert, and resolve these records rather than discard them.
- If a payment changes currency, ingestion rejects it. Production must model conversions as explicit linked transfers, not add amounts across currencies.
- This implementation sums per-payment captures and refunds. It does not model authorization, partial reversals against a particular capture, fees, FX conversion, chargebacks, or double-entry accounting. No reconciliation result certifies settlement.
- The model schema and evidence ID validation prevent some malformed output, not unsupported natural-language claims. A human must verify the brief. No real-ticket model accuracy has been measured.
- The public browser worker generates fictional events and independently demonstrates the concepts. It does not connect to the Python engine or a payment processor.

## AI evaluation plan

Build a labeled historical set with permission and source snapshots as they existed at decision time. Evaluate mismatch identification (deterministic), model claim support, citation precision, unsafe money-action suggestions, reviewer correction rate, and minutes to triage. Compare offline template and model-assisted briefs against the current operator workflow. Gate any real desk writeback on identity, audit, retention, and human approval. Do not use synthetic fixture pass rates as field accuracy.
