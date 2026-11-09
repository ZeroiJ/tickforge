# ADR-0002: Dedup key + out-of-order handling policy

| | |
|---|---|
| **Status** | Accepted (Phase 3, drills 3 & 4 pass — 2026-11-09) |
| **Date** | 2026-11-09 |
| **Implements** | plan.md Phase 3 "Deduplication" + "Out-of-order handling"; drills #3, #4 |

## Context

`.mkt` files are append-once and one per `(symbol, day)` (ADR-0001). The Phase 3
gate sits between the canonical Parquet and the writer, and must answer two questions
cheaply, deterministically, and loudly:

1. Which incoming rows are duplicates, and what is the key?
2. What happens to events that arrive out of time order ("late" events)?

Data reality: Binance **aggTrades** give every aggregate a unique `trade_id` per
`(exchange, symbol, day)`; trading-day files are naturally near-ordered but can
contain equal-timestamp aggregates (ties), and the drill contract (plan.md §7)
requires: 5 identical events → `inserted=1, dupes=4`; a late event must land in
its correct 1s bar with a `late_events` counter.

## Decision

**Dedup key = `trade_id`**, with an exact-payload fingerprint
`(ts, price.to_bits(), qty.to_bits())` kept for the first occurrence:

- same id + same fingerprint → duplicate, dropped (`duplicates_skipped++`);
- same id + **different** fingerprint → contract violation, `MktError::InvalidInput`
  → the ingest refuses to write (fail loudly, never repair);
- otherwise kept (`inserted++`).

**Ordering policy = sort on write.** Rows are time-ascending in every `.mkt` file;
`tf-engine::pipeline::dedup` returns survivors in *input* order, then a stable
argsort by `ts` permutes all columns in lockstep. Every input row that arrived
after a strictly newer row counts as `late_events` (drill #4 semantics: the
"now − 90s" event). Stable sort preserves arrival order for timestamp ties, which
makes `open`/`close` of a bar deterministic (stream order).

## Why not the alternatives

| Alternative | Why rejected |
|---|---|
| Dedup on `(symbol, ts, price, qty)` only | Collides legitimately: two different aggregates can share a price/qty second — would drop real data | 
| `HashMap<key, seen Set>` + spill-to-disk seen-set | YAGNI: per-file scale is ≤ ~1M aggTrades/day; a single `HashMap<i64, fp>` is ~tens of MB. Revisit only for a real multi-file dedupe sweep |
| Drop late events / ignore ordering | Breaks Drill 4 and corrupts bar `open|close` at ties; sorted-on-write keeps every consumer contract simple |
| Reject any out-of-order input | Real feeds arrive skewed; sorting is cheap and loss-free |
| Keep "first wins" silent for payload conflicts | A mutated payload under the same id means the source contract broke — repairing silently would hide upstream corruption |

## Consequences

- ➕ Drills 3 & 4 are one counters-each implementation, tested at engine level
  (`tests/engine.rs`) and pipeline level (`python/tickforge/drills.py`).
- ➕ Writer/reader time-order contract (spec §5/§6) now enforced at ingest, so
  scan code can `break` early past the window's upper bound.
- ➖ Ingest holds one `HashMap` and re-permutes columns: ~O(n) extra allocations
  per file — negligible at day scale (417k rows: unmeasurable vs zstd time).
- ➖ `trade_id` uniqueness is trusted as the domain contract; if a future source
  (another exchange) reuses ids, this ADR must be revisited with a `(exchange, id)` key.

## Evidence (2026-11-09)

- engine tests: `dedup_five_identical_events` (inserted=1, dupes=4),
  `late_event_lands_in_correct_bar_after_sort` (late=1, correct 1s bar)
- drills: `python -m tickforge.drills` → drill2/3/4 PASS (incl. kill mid-write
  leaving "no file at canonical path", rerun completes; full parity with DuckDB)
