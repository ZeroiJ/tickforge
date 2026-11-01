# ADR-0001: Custom `.mkt` columnar format instead of Parquet

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-11-01 |
| **Phase** | 0 (Foundation) |

## Context

`tickforge` must store and query 100GB+ of Binance tick data on a 15GB-RAM laptop. The obvious shortcut — Parquet + DuckDB — would work functionally, but it hides the exact mechanisms this project exists to learn and prove: indexing, chunk min/max skipping, memory-mapped zero-copy reads, and time-range pruning. DuckDB stays in the stack only as our *correctness oracle*, never the storage engine.

## Decision

Build a custom binary columnar format `.mkt`, read via `memmap2`, with:

- **Header + footer**: magic bytes `TFMK`, version, footer last (so readers check integrity before touching column data).
- **Column sections**: each column stored separately, chunked ~10⁶ rows; every chunk records `min/max` (enables `ts BETWEEN t1 AND t2` predicate skipping).
- **Encodings**: plain + `zstd` first; dictionary/RLE only where cheap (low-cardinality bools), per the Vortex per-column-encoding lesson.
- **Append-once, time-ordered writes**: fail loudly on corruption (`thiserror`), no query-time repair.

## Why not just Parquet?

1. **The engine is the project.** Hand-rolling pruning/indexing is where the learning (and the resume bullet) comes from — Parquet's footers would do it for us, leaving nothing to explain in an interview.
2. **We control RAM by construction.** Our access pattern is (column, time-range) slices of static files; reading exactly the needed chunks via memmap enforces <2GB RAM *by design*, not by trusting a dependency's planner.
3. **Scope fit.** We gain nothing from Parquet's schema evolution, row-group rewrite, or multi-file entity machinery: tick files are written once per `(symbol, day)` and stuck.

## Consequences

- ➕ Full control and instrumentation; every query cost is explainable.
- ➕ Same-footprint read path is teachable line-by-line (memmap + footer-first).
- ➖ We own correctness (checksums, kill/resume) — covered by must-pass drills 2, 5, 6.
- ➖ No ecosystem interop for `.mkt`; mitigated by the Parquet sidecar (`data/norm/`) that remains queryable by DuckDB and exports to Delta/Databricks.
- ➖ Slow start vs "pip install duckdb" — accepted; honest benchmark plan requires a real competitor, and DuckDB/Parquet *is* that competitor.

## Alternatives considered

| Alternative | Why rejected |
|---|---|
| Parquet only, no custom format | No pruning/indexing learning; nothing to bench honestly in Rust. |
| DuckDB `.duckdb` file | Row-store-on-disk engine; it *is* the challenger, not the storage layer. |
| Arrow IPC / Feather | Decides layout (incl. encodings) for us; less custom learning, same "explain it" problem. |
| SQLite row file | Wrong orientation for bulk column scans at tick granularity. |

## References

- `plan.md` §2 (Vortex per-column encodings research note)
- Upcoming: `docs/format-spec.md` (Phase 2), `docs/benchmarks.md` (Phase 3)
