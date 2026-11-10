//! `tf-engine` — the tickforge query engine (plan.md Phase 3).
//!
//! Three responsibilities, each small and provable:
//! 1. [`scan`]   — `ts BETWEEN t1 AND t2` scans over `.mkt` files that touch ONLY the
//!                 chunks overlapping the window (chunk min/max pruning via
//!                 `MktFile::chunks_for_ts_range`), then row-filter inside those chunks.
//! 2. [`resample`] — trade events → OHLCV bars (any bucket size, default 1s),
//!                 including taker-side split (`is_buyer_maker` ⇒ the resting buyer
//!                 was the maker, so the aggressor was the seller).
//! 3. [`pipeline`] — ingest-side data-quality gate: exact-duplicate removal keyed on
//!                 `trade_id` (plan.md Drill 3) and time-ordering with a `late_events`
//!                 counter for events that arrived after a strictly newer event
//!                 (plan.md Drill 4).

pub mod pipeline;
pub mod resample;
pub mod scan;

pub use pipeline::{DedupStats, OrderStats, RowTable, dedup};
pub use resample::{Bar, resample};
pub use scan::{Engine, Scanned, ScanStats, for_each_window_chunk, scan_ts_range};

/// Time ORDER stats for a scan window (events are always sorted in file order).
pub const US_PER_SEC: i64 = 1_000_000;
