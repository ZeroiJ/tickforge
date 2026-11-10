//! Time-range scans (`ts BETWEEN t1 AND t2`) â€” the Phase 3 pruning proof.
//!
//! Pruning is two-leveled:
//! * chunk level: a chunk overlaps the window iff its stored ts min/max overlap
//!   (`MktFile::chunks_for_ts_range`) â€” all other chunks are skipped *without*
//!   their payload bytes being touched;
//! * row level: inside the overlapping chunks, rows are filtered by
//!   `t1 <= ts <= t2` (both inclusive; timestamps are canonical Âµs).

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
    pub fn is_empty(&self) -> bool {
        self.ts.is_empty()
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

/// Scan `f` for events with `t1 <= ts <= t2` (inclusive, Âµs).
pub fn scan_ts_range(f: &MktFile, t1: i64, t2: i64) -> Result<(Scanned, ScanStats), MktError> {
    let mut out = Scanned::default();
    let mut stats = for_each_window_chunk(f, t1, t2, |mut part| {
        out.ts.append(&mut part.ts);
        out.trade_id.append(&mut part.trade_id);
        out.price.append(&mut part.price);
        out.qty.append(&mut part.qty);
        out.quote_qty.append(&mut part.quote_qty);
        out.flags.append(&mut part.flags);
        Ok(())
    })?;
    stats.rows_emitted = out.row_count();
    Ok((out, stats))
}

/// Streaming scan (Phase 3 / Drill 5): the chunk-level sibling of
/// [`scan_ts_range`]. Emits one row-filtered [`Scanned`] per overlapping chunk
/// and touches NOTHING else â€” peak memory stays at one â‰¤10â¶-row chunk (~40MB
/// window of column vectors) no matter how big the file set is. This is the
/// RAM-by-construction lever.
pub fn for_each_window_chunk<F>(f: &MktFile, t1: i64, t2: i64, mut emit: F) -> Result<ScanStats, MktError>
where
    F: FnMut(Scanned) -> Result<(), MktError>,
{
    let candidates = f.chunks_for_ts_range(t1, t2)?;
    let total = f.n_chunks(COL_TS)?;
    let mut chunks_scanned = 0usize;
    let mut chunks_skipped = total - candidates.len();
    let mut rows_decoded = 0usize;
    let mut rows_emitted = 0usize;

    for &chunk in &candidates {
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
        chunks_scanned += 1;
        let mut part = Scanned::default();
        for i in 0..ts.len() {
            if ts[i] > t2 {
                // file rows are time-ascending; everything after is out of window
                break;
            }
            if ts[i] < t1 {
                continue;
            }
            part.ts.push(ts[i]);
            part.trade_id.push(trade_id[i]);
            part.price.push(price[i]);
            part.qty.push(qty[i]);
            part.quote_qty.push(quote_qty[i]);
            part.flags.push(flags[i]);
        }
        if !part.is_empty() {
            rows_emitted += part.row_count();
            emit(part)?;
        }
        // chunk-level drop: vectors go out of scope here (DRAM peak = one chunk)
    }

    Ok(ScanStats {
        chunks_scanned: chunks_scanned,
        chunks_skipped: chunks_skipped,
        rows_decoded: rows_decoded,
        rows_emitted: rows_emitted,
    })
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
