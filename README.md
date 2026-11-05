# tickforge

> A tick-level crypto market microstructure lab: a custom memory-mapped columnar engine (`.mkt`) that queries 100GB+ of Binance tick data with **<2GB RAM**, proved with drills — not vibes.

**Status:** 🚧 **Phase 2 — `.mkt` engine format** ✅ *(2026-11-05: format spec v1, writer+memmap reader, ingest & round-trip proofs — see Results)*

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
| 2 | Kill & resume mid-write, no corruption | 🚧 designed for (atomic tmp+fsync+rename; footer-last), drill run pending Phase 3 |
| 3 | Deduplication (`inserted=1, dupes=4`) | ⬜ pending Phase 3 |
| 4 | Late/out-of-order trade lands in correct bar | ⬜ pending Phase 3 |
| 5 | RAM-safe scan (24h, <2GB RAM, correct vs DuckDB) | ⬜ pending Phase 3 |
| 6 | Replay integrity (1x replay bars == full-pass bars) | ⬜ pending Phase 3 |

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

## Docs

- [`plan.md`](plan.md) — full roadmap, phases 0–6
- [`docs/ADR-0001-custom-mkt-format.md`](docs/ADR-0001-custom-mkt-format.md) — why not Parquet
- [`docs/format-spec.md`](docs/format-spec.md) — `.mkt` v1 byte-level spec
- [`docs/benchmarks.md`](docs/benchmarks.md) — sizes, ratios, drill numbers

## Bugs I hit

> Documented failures go here — debugging skill is the point.
