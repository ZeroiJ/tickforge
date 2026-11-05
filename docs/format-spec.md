# `.mkt` Format Spec — v1

| | |
|---|---|
| **Status** | Accepted (first passing round-trip, 2026-11-05) |
| **Date** | 2026-11-05 |
| **Implements** | ADR-0001 (custom columnar format) |
| **Canonical schema** | `rust/crates/tf-core/src/schema.rs` ↔ `python/tickforge/schema.py` (SCHEMA_VERSION 1) |

> Design goals (from ADR-0001): footer-first integrity check, per-chunk `ts` min/max for
> range pruning, memmap zero-copy reads, fail loudly on corruption, append-once writes
> made kill-safe by **write-tmp → fsync → rename**.

## 0. File layout (v1)

```text
┌─────────────────────────────┐
│ header (8 B)                │ magic + version
│ column sections             │ columns in id order, each = chunks in row order
│ footer body (variable)      │ metadata + per-column chunk index
│ footer_len (4 B, u32 LE)    │ sizeof(footer body)
│ magic "TFMK" (4 B)          │ last 8 bytes of the file, always
└─────────────────────────────┘
```

Reader order is fixed: **header → footer → (only then) column data**. A file whose last
4 bytes are not `TFMK` (e.g. killed mid-write before rename) is rejected before any
column read — that is Drill-2's recovery guarantee at the format level.

## 1. Header — 8 bytes

| offset | size | field | notes |
|---|---|---|---|
| 0 | 4 | `magic` = `"TFMK"` | 0x54 0x46 0x4D 0x4B |
| 4 | 2 | `format_version: u16 LE` | currently `1` |
| 6 | 2 | **reserved**, must be `0x0000` | keeps the header at 8 bytes |

## 2. Physical columns

One `.mkt` file = one `(symbol, source day)` pair, written once. Two schema fields are
*pulled out* of the columnar section rather than stored per-row (v1 decision):

- `symbol` — constant for the whole file → stored once in the footer.
- `is_buyer_maker` + `is_best_match` — two low-cardinality bools → bit-packed into one
  `flags: u8` column (`bit0 = is_buyer_maker`, `bit1 = is_best_match`).

Canonical field order (schema.py — 8 logical fields):

| col id | field | physical | rustic type | payload encoding |
|---|---|---|---|---|
| 0 | `trade_id` | i64 | `i64` | plain / zstd |
| 1 | `price` | f64 | `f64` | plain / zstd |
| 2 | `qty` | f64 | `f64` | plain / zstd |
| 3 | `quote_qty` | f64 | `f64` | plain / zstd |
| 4 | `ts` | i64 µs | `i64` | plain / zstd |
| 5 | flags | u8 | 2×bool packed | plain / zstd |

All integers/floats are **little-endian**, arrays are **contiguous native-width** values
(no varint, no framing) — v1 keeps decode trivially correct; fancier per-column encodings
(dictionary/RLE/frame-of-reference) are v2+ per the Vortex note in `plan.md` §2.

## 3. Chunks

- A column is split into chunks of at most `chunk_rows_max = 1_000_000` rows
  (the ~10⁶-row target in ADR-0001). The final chunk may be shorter.
- Chunk boundaries are **aligned across columns**: chunk *k* covers rows
  `[k·chunk_rows_max, min((k+1)·chunk_rows_max, row_count))` in every column.
  This is what makes per-column reads with a shared row predicate trivial.
- Each chunk payload = the plain/zstd-compressed concatenation of that chunk's values,
  in row order, native width, little-endian.

## 4. Footer body

Big-endian *nothing*: everything below is little-endian. Order:

```text
meta block:
  magic "TFMK"            4
  format_version  u16     (= 1, must equal header)
  schema_version  u32     (= canonical SCHEMA_VERSION, currently 1)
  row_count       u64
  chunk_rows_max  u32
  ts_min          i64     file-level µs
  ts_max          i64     file-level µs
  symbol_len      u16 + symbol bytes (UTF-8, e.g. b"BTCUSDT")

column index (n_columns ⊂ {0..5}, ascending col id; here n_columns = 6):
  per column:
    column_id     u8
    codec         u8       (0 = plain, 1 = zstd)
    n_chunks      u32
    per chunk (fixed 52 B):
      start_row   u64
      row_count   u32
      min         i64      raw min of the chunk's VALUES (as bit-pattern for f64 payloads)
      max         i64      raw max (same convention)
      data_offset u64      absolute byte offset of the payload
      comp_len    u64      length of stored payload bytes
      uncomp_len  u64      length after decode (bytes)
```

**min/max convention**: stored as raw i64. For integer columns that is the numeric
min/max. For f64 columns the *bit pattern* is compared as an i64 (this is a documented
v1 simplification — total-order true for non-negative floats, which tick prices/qty are;
the writer still computes min/max from decoded values at query time when pruning must be
exact). Only the `ts` column's min/max drives v1 pruning.

## 5. Reader contract (`tf-format::reader`)

1. memmap the file (memmap2; **never** read_to_end — dev rule in AGENTS.md §4).
2. Verify last 4 bytes = `TFMK`, read `footer_len`, slice + parse footer body fully,
   cross-check `format_version`, `schema_version`, chunk arithmetic sums to `row_count`.
3. Column reads go only through the index: decode exactly the chunks overlapping the
   requested row/time window. `read_ts(t1, t2)` returns, per chunk, the decoded `ts`
   values — later phases use these per-chunk bounds to skip without touching payloads.

Error type: `thiserror`, one variant per corruption class (`BadMagic`, `VersionMismatch`,
`FooterTruncated`, `IndexMismatch`, `CodecUnknown`). Never silently repaired.

## 6. Writer contract (`tf-format::writer`)

- Input: rows in canonical field order (from the normalized Parquet via `tf-cli`).
- Output is **atomic**: write to `<path>.tmp`, flush, fsync, rename onto `<path>`.
  A killed process at any point leaves either the old file or the tmp — never a
  half-valid `.mkt` (this is the mechanism behind Drill 1 idempotency + Drill 2).
- Recompute per-chunk min/max while writing; no trusting caller-supplied stats.

## 7. Round-trip acceptance (Phase 2 exit)

`tf-cli roundtrip` on the real day (`BTCUSDT aggTrades 2026-10-31`, 417,528 rows):

- every column, every row: decoded value == Parquet memory value **bitwise** (f64 with
  NaN never occurring in this data; integers exact),
- row_count identical; `ts_min`/`ts_max` identical to `stats.json`,
- sizes logged raw-vs-`.mkt` into `docs/benchmarks.md` (target 4–8× vs the 34.45 MB CSV).
