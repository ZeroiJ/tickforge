# tickforge

> A tick-level crypto market microstructure lab: a custom memory-mapped columnar engine (`.mkt`) that queries 100GB+ of Binance tick data with **<2GB RAM**, proved with drills — not vibes.

**Status:** 🚧 **Phase 3 — queries, drills 1–6 ✅; replay done. Remaining: Sprint-2 scale (drill 5 @ 24h/5 symbols, multi-chunk bench)** *(2026-11-09: tf-engine scan/resample/dedup, tf-cli query+replay, all six drills pass — see Proofs)*

---

## Problem

Raw Binance tick data doesn't get treats like bars or candles — it's millions of rows/day with duplicates, out-of-order timestamps, and gaps. Batch tools pretend real-time structure doesn't exist. This repo builds the ingestion, storage, and query layer that trading systems actually face, then *proves* correctness with kill/resume, dedup, late-data, and RAM-safety drills.

## Design

- **Rust** (`tf-core` → `tf-format` → `tf-engine` → `tf-cli`): `.mkt` binary columnar format, `memmap2` reads, footer/index-first, `ts` range pruning.
- **Python**: download from `data.binance.vision`, normalize → Parquet, verification vs DuckDB, drills.
- **R** (`data.table` / `ggplot2`): one falsifiable microstructure hypothesis with effect size, not just plots.
- **DuckDB**: sanity checker, not the storage engine.
- **Databricks / Delta**: normalized data lands in Volumes with industry partitioning (`exchange/symbol/dt=`).

Why a custom format instead of Parquet → see [`docs/ADR-0001-custom-mkt-format.md`](docs/ADR-0001-custom-mkt-format.md).

## Layout

```text
tickforge/
├── docs/           # ADRs, format spec, benchmarks
├── rust/           # tf-core · tf-collect · tf-format · tf-engine · tf-cli
├── python/         # download · normalize · verify · drills
├── r/              # microstructure research + report
└── data/           # gitignored: raw/ · norm/ · mkt/ · lake/
```

## Stack

| Layer | Choice |
|---|---|
| Engine | Rust stable (1.99+) — `memmap2`, `zstd`, `thiserror` |
| Glue | Python 3.11+ — `polars`, `pyarrow`, `duckdb`, `typer` |
| Research | R 4.x — `data.table`, `zoo/xts`, `ggplot2` |
| Lakehouse | Delta Lake → Databricks Volumes |

## Proofs

> The 6 must-pass drills (plan.md §7). Filled in as phases complete — no empty claims.

| # | Drill | Result |
|---|---|---|
| 1 | Idempotent ingest (2nd run `written=0`) | ✅ `tf-cli ingest-norm` ×2 → 2nd run `rows_written=0` (sha-keyed skip) |
| 2 | Kill & resume mid-write, no corruption | ✅ kill mid-write → nothing at canonical path (atomic rename); rerun completes; state idempotent |
| 3 | Deduplication (`inserted=1, dupes=4`) | ✅ 5 identical events → `rows_written=1 duplicates_skipped=4`, count unchanged (`python -m tickforge.drills`) |
| 4 | Late/out-of-order trade lands in correct bar | ✅ late event → `late_events=1`, sole trade of its correct 1s bar; bars ≡ DuckDB |
| 5 | RAM-safe scan (24h, <2GB RAM, correct vs DuckDB) | 🚧 1-day scale proven: full-day scan+resample peak WS **46.2 MiB**, 77,136 1s bars ≡ DuckDB; 24h/5-symbol scale pending Sprint 2 data |
| 6 | Replay integrity (1x replay bars == full-pass bars) | ✅ full-day max-rate replay → **77,136 streamed bars exactly equal** full-pass bars; paced mode verified (100×, `pacing_x=1.00`) |

## Results

> Benchmarks (query latency, compression ratio, rows/sec) go to [`docs/benchmarks.md`](docs/benchmarks.md) and get summarized here once real numbers exist.

**First proof (Phase 1 ingest chain, 2026-10-31 · BTCUSDT · aggTrades):**
- `data.binance.vision` zip **6.11MB** → CSV **34.45MB**, 417,528 aggregates, sha256-verified against `.CHECKSUM`
- canonical Parquet (zstd): **4.92MB** — DuckDB-verified `count/price-bounds/ts-range/Σqty` == raw CSV
- second normalize run: `rows_written=0` (idempotent, sha-keyed)
- contract: timestamps **µs** (spot ≥2025), `quote_qty` derived `price*qty`, tests: 26 pytest + 2 cargo green

**Phase 2 (2026-11-05 · custom `.mkt` format):**
- first `.mkt` ingest: CSV 34.45MB → **4.74MB** `.mkt` (**7.6×**), 417,528 rows, `tf-cli ingest-norm`
- round-trip: `.mkt` decoded → **100% bitwise match** vs Parquet across all 6 columns (`tf-cli roundtrip`)
- correctness stack: 7 format tests (multi-chunk round trip, byte-deterministic rewrite, corrupted magic/footer/payload all fail loudly) + 26 pytest + 2 core-crate tests
- docs: [`format-spec.md`](docs/format-spec.md) v1 · [`benchmarks.md`](docs/benchmarks.md)

**Phase 3 (2026-11-09 · query engine + drills):**
- [`tf-engine`](rust/crates/tf-engine): time-range scan (2-level pruning: chunk min/max → row filter), 1s OHLCV resample with taker-side split, dedup (`trade_id` + payload fingerprint) and sort-on-write with `late_events` counter
- `tf-cli query --from/--to [--resample N --out bars.parquet]`: 10-min window **90 ms**; full-day scan+resample **176 ms** → **77,136 1s bars**, peak working set **46.2 MiB**
- `tf-cli replay --speed N` (max-rate integrity mode = Drill 6): streamed incremental bars **exactly equal** full-pass bars; paced 60s @100× → 747ms wall vs 593ms scheduled (self-caught: first version computed pacing but never slept — drill caught it via wall-time assertion)
- bar correctness: **77,136/77,136 bars match DuckDB** — the drill exposed DuckDB's nondeterministic ties at equal-µs timestamps (1341 mismatches) → fixed with stream-order tiebreak
- drills as code: `python -m tickforge.drills` → **drills 1–6 PASS** (idempotent, kill-mid-write, dedup 1+4, late-correct-bar, DuckDB parity, replay equality + pacing)
- design: [`ADR-0002-dedup-key-choice.md`](docs/ADR-0002-dedup-key-choice.md) · numbers: [`benchmarks.md`](docs/benchmarks.md)

## Docs

- [`plan.md`](plan.md) — full roadmap, phases 0–6
- [`docs/ADR-0001-custom-mkt-format.md`](docs/ADR-0001-custom-mkt-format.md) — why not Parquet
- [`docs/format-spec.md`](docs/format-spec.md) — `.mkt` v1 byte-level spec
- [`docs/benchmarks.md`](docs/benchmarks.md) — sizes, ratios, drill numbers

## Bugs I hit

> Documented failures go here — debugging skill is the point.

- **Phase 2 — writer/reader 2-byte offset lag**: missing reserved header field; reader failed loudly exactly as designed.
- **Phase 3 — DuckDB tie nondeterminism**: `last(price ORDER BY ts)` picked arbitrary rows among equal-µs aggTrades → 1341 bar mismatches vs tf bars; fixed with stream-order tiebreak (`file_row_number=true`).
- **Phase 3 — replay pacing that never slept**: `sleep_for()` returned computed durations used only as an accumulator (`wall=0ms`, fabricated `pacing_x=1.00`); drill's wall-time assertion caught it → now really `thread::sleep`s.
- **Phase 3 — silent source drift**: `data/norm` canonical parquet was at some point replaced by a 3-row file on disk; sha-keyed idempotency meant every downstream proof was shrinking with it. Rebuilt from the raw CSV via the Phase-1 pipeline (`normalize_day.py`, DuckDB-verified) and drill 1 now guards source row count (417,528). Lesson: idempotency checks should also assert the *source* is the expected shape.
