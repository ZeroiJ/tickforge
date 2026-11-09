//! Time-range scans (`ts BETWEEN t1 AND t2`) — the Phase 3 pruning proof.
//!
//! Pruning is two-leveled:
//! * chunk level: a chunk overlaps the window iff its stored ts min/max overlap
//!   (`MktFile::chunks_for_ts_range`) — all other chunks are skipped *without*
//!   their payload bytes being touched;
//! * row level: inside the overlapping chunks, rows are filtered by
//!   `t1 <= ts <= t2` (both inclusive; timestamps are canonical µs).

use tf_format::{ColData, MktFile, COL_FLAGS, COL_PRICE, COL_QTY, COL_QUOTE_QTY, COL_TRADE_ID, COL_TS};
use tf_format::error::MktError;

/// Column material from one scan window, row-filtered, in file (time) order.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Scanned {
    pub ts: Vec<i64>,
    pub trade_id: Vec<i64>,
    pub price: Vec<f64>,
    pub qty: Vec<f64>,
    pub quote_qty: Vec<f64>,
    pub flags: Vec<u8>,
}

impl Scanned {
    pub fn row_count(&self) -> usize {
        self.ts.len()
    }
}

/// Evidence for the honest benchmark table (docs/benchmarks.md): how much we
/// skipped is as interesting as the answer itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanStats {
    /// chunks whose payloads were decoded (overlap [t1, t2])
    pub chunks_scanned: usize,
    /// ts-chunks skipped by min/max without reading their payload
    pub chunks_skipped: usize,
    /// rows inside scanned chunks (payload cost, before row filter)
    pub rows_decoded: usize,
    /// rows actually inside [t1, t2] (the answer)
    pub rows_emitted: usize,
}

/// Scan `f` for events with `t1 <= ts <= t2` (inclusive, µs).
pub fn scan_ts_range(f: &MktFile, t1: i64, t2: i64) -> Result<(Scanned, ScanStats), MktError> {
    let candidates = f.chunks_for_ts_range(t1, t2)?;
    let total_ts_chunks = f.n_chunks(COL_TS)?;

    let mut out = Scanned::default();
    let mut rows_decoded = 0usize;

    for &chunk in &candidates {
        // chunk boundaries are aligned across columns (spec §3), so chunk k of
        // every column holds the same row slice.
        let ts = col_i64(f.decode_chunk(COL_TS, chunk)?)?;
        let trade_id = col_i64(f.decode_chunk(COL_TRADE_ID, chunk)?)?;
        let price = col_f64(f.decode_chunk(COL_PRICE, chunk)?)?;
        let qty = col_f64(f.decode_chunk(COL_QTY, chunk)?)?;
        let quote_qty = col_f64(f.decode_chunk(COL_QUOTE_QTY, chunk)?)?;
        let flags = match f.decode_chunk(COL_FLAGS, chunk)? {
            ColData::U8(v) => v,
            _ => return Err(MktError::TypeMismatch { id: COL_FLAGS, expected: "u8" }),
        };

        rows_decoded += ts.len();
        for i in 0..ts.len() {
            if ts[i] > t2 {
                // file rows are time-ascending; everything after is out of window
                break;
            }
            if ts[i] < t1 {
                continue;
            }
            out.ts.push(ts[i]);
            out.trade_id.push(trade_id[i]);
            out.price.push(price[i]);
            out.qty.push(qty[i]);
            out.quote_qty.push(quote_qty[i]);
            out.flags.push(flags[i]);
        }
    }

    let stats = ScanStats {
        chunks_scanned: candidates.len(),
        chunks_skipped: total_ts_chunks - candidates.len(),
        rows_decoded,
        rows_emitted: out.row_count(),
    };
    Ok((out, stats))
}

/// [`scan_ts_range`] bound to an open file, for call-site ergonomics.
pub struct Engine<'a> {
    f: &'a MktFile,
}

impl<'a> Engine<'a> {
    pub fn new(f: &'a MktFile) -> Self {
        Self { f }
    }

    pub fn scan(&self, t1: i64, t2: i64) -> Result<(Scanned, ScanStats), MktError> {
        scan_ts_range(self.f, t1, t2)
    }
}

fn col_i64(d: ColData) -> Result<Vec<i64>, MktError> {
    match d {
        ColData::I64(v) => Ok(v),
        _ => Err(MktError::TypeMismatch { id: COL_TS, expected: "i64" }),
    }
}

fn col_f64(d: ColData) -> Result<Vec<f64>, MktError> {
    match d {
        ColData::F64(v) => Ok(v),
        _ => Err(MktError::TypeMismatch { id: COL_PRICE, expected: "f64" }),
    }
}
