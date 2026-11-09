# tickforge Benchmarks

> Honest ledger: every claim comes from a command you can rerun. Machine: Ryzen laptop, 15GB RAM, NVMe. All numbers append-only as phases complete.

## Phase 2 — `.mkt` format round-trip (2026-11-05 | BTCUSDT aggTrades 2026-10-31)

Fixture: the Phase-1 canonical day — 417,528 rows, single chunk (417,528 ≤ 10⁶ chunk cap), zstd payload codec, deterministic encode.

| metric | value |
|---|---|
| rows | 417,528 |
| `.mkt` file size | 4,736,852 B (4.52 MiB) |
| uncompressed column bytes | 17,118,648 B |
| compression ratio (uncomp → `.mkt`) | 3.61× |
| raw CSV (source of truth) | 34,451,014 B |
| **CSV → `.mkt` ratio** | **7.6×** (target 4–8×, hit) |
| normalized parquet | 5,156,191 B — `.mkt` is 1.09× smaller (parquet stores `symbol` per row + row-group bookkeeping) |
| round-trip | 417,528 rows, 6 columns, **100% bitwise match** vs Parquet (incl. every f64 bit pattern) |
| re-write determinism | byte-identical output for identical input (zstd deterministic, level 3 pinned) |
| footer-first integrity | corrupted trailing magic / truncated footer / corrupted payloads → rejected or loud decode error (7 tests) |

Commands:

```
cargo test -p tf-format                                    # 7 round-trip + corruption tests
tf-cli ingest-norm  --input data/norm/.../trades.parquet   # rows_written=417528 (1st)
tf-cli ingest-norm  --input ...                            # rows_written=0 (2nd, sha-keyed skip)
tf-cli roundtrip    --input ...                            # ALL-BITWISE-MATCH
```

Notes:
- The 2 extra header bytes bit us once (writer offset vs disk lagged by 2 — index arithmetic failed loudly in the reader exactly as designed; root cause was the reserved header field missing from the writer). Format spec §1 now pins it.
- RAM footprint during both commands stayed at ~few MiB (417k-row file); the 24h/5-symbol RAM drill comes in Phase 3.

## Phase 3 — query engine, drills 2–5 (2026-11-09 | BTCUSDT aggTrades 2026-10-31)

Fixture: same 417,528-row day, `.mkt` single chunk (rows < 10⁶ chunk cap — so
chunk-skip counters read `skipped=0` here; pruning behavior is proven per-chunk in
`tf-engine` tests with 10-chunk fixtures). Release build (`cargo build --release`).

| metric | value |
|---|---|
| full-day scan + 1s OHLCV (tf-cli, in-process) | **176 ms** — 77,136 bars (≈77% of seconds have trades) |
| full-day reference bars (DuckDB on parquet) | 139 ms |
| 10-min window scan (`tf-cli query`) | **90 ms** in-process (**110 ms** wall incl. process spawn) — 10,834 rows |
| full-day max-rate replay (Drill 6 integrity mode) | 417,528 events streamed; **77,136 streamed-incremental 1s bars EXACTLY equal** to full-pass `query` bars |
| paced replay | 60s event window @100× → wall 747ms vs 593ms scheduled (`pacing_x=1.00`, 652 events) |
| peak working set during full-day resample | **46.2 MiB** (Task-Manager-style WS sampling; <2GB target hit at 1-day scale) |
| ingest w/ dedup+sort gate | unchanged byte-effective sizes; adds counters `duplicates_skipped`, `late_events` to stats.json |

**Drills (plan.md §7), run via `python -m tickforge.drills` (results.json):**

| # | drill | result |
|---|---|---|
| 2 | kill & resume | ✅ kill mid-write → **no file at canonical path** (atomic rename contract); rerun completes; completed state idempotent |
| 3 | dedup | ✅ 5 identical events → `rows_written=1 duplicates_skipped=4`, `.mkt` count unchanged |
| 4 | late/out-of-order | ✅ late event (appended last, ts = −1.5s behind stream) → `late_events=1`, lands as sole trade of its correct 1s bar; bars ≡ DuckDB |
| 5 | RAM-safe scan (bar-correctness part) | ✅ full-day 1s bars match DuckDB (OHLC/count exact); true 24h/5-symbol scale + Task Manager drill pending Sprint 2 data |
| 6 | replay integrity (1x replay bars == full-pass bars) | ✅ max-rate replay of the full day (417,528 events, incremental aggregation) → 77,136 bars **exactly** equal full-pass bars; paced mode verified at 100× (`pacing_x=1.00`). Gaps > 10s are jumped (`gaps_skipped`) so historical replays don't stall — documented behavior, not hidden |

**Bug the drill caught (documented failure):** DuckDB's `last(price ORDER BY ts)`
is nondeterministic when several aggTrades share the same µs timestamp — first run
of the bar-equality check showed **1341 close mismatches** in `tf` bars. Root cause:
tie order, not bar math. Fix in the *reference query*: `read_parquet(..., file_row_number=true)`
+ `ORDER BY ts, file_row_number` (stream-order tiebreak, same as `tf` bars).
Afterwards: 77,136/77,136 bars match (OHLC/count exact, volumes within fp tolerance).

Commands:

```
cargo test                # 15 tests (2 core + 7 format + 6 engine)
python -m tickforge.drills   # drills 1–6, writes data/tmp/drills/results.json
tf-cli query  --mkt <.mkt> --from <µs> --to <µs> [--resample 1 --out bars.parquet]
tf-cli replay --mkt <.mkt> --from <µs> --to <µs> [--speed 100] [--resample 1 --out bars.parquet]
```

**Replay pacing bug (self-caught):** the first drill-6 run reported `wall=0ms` while
claiming `pacing_x=1.00` — `sleep_for()` computed pacing durations but *never slept*
(unit-name confusion: it returned a Duration used only as an accumulator). The drill
caught it because wall-time was asserted, not the fabricated ratio. Now threads really
sleep; wall (747ms) ≈ scheduled (593ms) at 100× + per-call sleep quantum overhead.

Honest note: DuckDB still beats `tf-cli` end-to-end on this whole-file resample
(139ms vs 176ms) — it uses parquet statistics and multithreading. `tf-cli`'s edge
is the pruned *window* scan (90ms w/o spawn overhead) and the RAM-by-construction
footprint; the multi-chunk/multi-day Sprint-2 benchmark will show where pruning
overshadows full-scan speed.
