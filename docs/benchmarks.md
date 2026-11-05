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
