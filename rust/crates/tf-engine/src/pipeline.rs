//! Ingest-side data-quality gate: dedup (plan.md Drill 3) + time-ordering with
//! late-event counting (plan.md Drill 4).
//!
//! Policy (ADR-0002):
//! * dedup key = `trade_id` (unique per (exchange, symbol, day) in aggTrades).
//!   An incoming row whose `trade_id` was already seen AND whose payload
//!   (ts, price, qty) is identical to the first occurrence is a duplicate → dropped.
//!   Same id with a DIFFERENT payload is a contract violation → error (fail loudly).
//! * rows are written time-ascending; an event that arrives after a strictly newer
//!   event is "late" → sorted into place, `late_events` counts how many.
//!
//! Implementation notes: canonical rows live in parallel Vecs (columnar), so
//! dedup/sort operate on indices and then gather every column with the same
//! permutation — safe `Vec` gathers, lane's data is 417k rows/day-scale, not
//! byte-tight.

use std::collections::HashMap;

use tf_format::error::MktError;

/// Row table in canonical column order (minus `symbol`, which lives in the
/// `.mkt` footer): schema.rs field order projected onto parallel arrays.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RowTable {
    pub trade_id: Vec<i64>,
    pub ts: Vec<i64>,
    pub price: Vec<f64>,
    pub qty: Vec<f64>,
    pub quote_qty: Vec<f64>,
    pub flags: Vec<u8>,
}

impl RowTable {
    pub fn len(&self) -> usize {
        self.trade_id.len()
    }
    pub fn is_empty(&self) -> bool {
        self.trade_id.is_empty()
    }

    /// Gather rows by `perm` into a new table: result[j] = rows[perm[j]].
    fn gather(&self, perm: &[usize]) -> RowTable {
        RowTable {
            trade_id: permute_i64(&self.trade_id, perm),
            ts: permute_i64(&self.ts, perm),
            price: permute_f64(&self.price, perm),
            qty: permute_f64(&self.qty, perm),
            quote_qty: permute_f64(&self.quote_qty, perm),
            flags: permute_u8(&self.flags, perm),
        }
    }
}

fn permute_i64(v: &[i64], perm: &[usize]) -> Vec<i64> {
    perm.iter().map(|&i| v[i]).collect()
}
fn permute_f64(v: &[f64], perm: &[usize]) -> Vec<f64> {
    perm.iter().map(|&i| v[i]).collect()
}
fn permute_u8(v: &[u8], perm: &[usize]) -> Vec<u8> {
    perm.iter().map(|&i| v[i]).collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DedupStats {
    pub inserted: usize,
    pub duplicates: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct OrderStats {
    /// events that arrived after a strictly newer event (sorted into place)
    pub late_events: usize,
}

/// (1) drops exact duplicates (key: `trade_id` + identical ts/price/qty payload),
/// (2) returns the surviving rows **still in input order** (caller sorts after),
/// plus the (stats, kept) pair.
///
/// Duplicate policy: first occurrence wins; a `trade_id` collision with a
/// different payload fingerprint is a hard error (`InvalidInput`) — never repair.
pub fn dedup(rows: &RowTable) -> Result<(DedupStats, RowTable), MktError> {
    let n = rows.len();
    // alignment of the parallel arrays must hold, or a dup check would compare
    // unrelated cells
    if n != rows.ts.len() || n != rows.price.len() || n != rows.qty.len() {
        return Err(MktError::InvalidInput("row table columns are misaligned".into()));
    }

    // key: trade_id -> fingerprint of the first occurrence
    // (i64 ts has the exact µs stamp; f64 payload summarized by bit pattern)
    let mut seen: HashMap<i64, (i64, u64, u64)> = HashMap::with_capacity(n);
    let mut keep: Vec<usize> = Vec::with_capacity(n);
    let mut stats = DedupStats::default();

    for i in 0..n {
        let fp = (rows.ts[i], rows.price[i].to_bits(), rows.qty[i].to_bits());
        match seen.entry(rows.trade_id[i]) {
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(fp);
                keep.push(i);
                stats.inserted += 1;
            }
            std::collections::hash_map::Entry::Occupied(e) => {
                if *e.get() == fp {
                    stats.duplicates += 1; // exact dup -> drop silently
                } else {
                    return Err(MktError::InvalidInput(format!(
                        "trade_id {} collides with different (ts,price,qty) payload — \
                         contract violation, refusing to ingest",
                        rows.trade_id[i]
                    )));
                }
            }
        }
    }

    let out = rows.pick(&keep);
    debug_assert_eq!(out.len(), keep.len());
    Ok((stats, out))
}

impl RowTable {
    /// Keep rows at `keep` (must be strictly ascending input indices) in order.
    fn pick(&self, keep: &[usize]) -> RowTable {
        RowTable {
            trade_id: permute_i64(&self.trade_id, keep),
            ts: permute_i64(&self.ts, keep),
            price: permute_f64(&self.price, keep),
            qty: permute_f64(&self.qty, keep),
            quote_qty: permute_f64(&self.quote_qty, keep),
            flags: permute_u8(&self.flags, keep),
        }
    }

    /// Stable sort by `ts` (ascending). Returns [`OrderStats`]; late_events is the
    /// count of input rows that arrived after a strictly newer row had already
    /// been seen (plan.md Drill 4 semantics).
    pub fn sort_row_ts(&mut self) -> OrderStats {
        let n = self.len();
        if n == 0 {
            return OrderStats::default();
        }
        let mut max_ts = self.ts[0];
        let mut late = 0usize;
        for &t in &self.ts {
            if t < max_ts {
                late += 1;
            } else {
                max_ts = t;
            }
        }

        // stable argsort is a permutation of 0..n by key ts (ties keep input order)
        let mut perm: Vec<usize> = (0..n).collect();
        perm.sort_by_key(|&i| self.ts[i]); // Vec sort_by_key is stable

        *self = self.gather(&perm);
        keep_runs(&self.ts, "sort_row_ts");
        OrderStats { late_events: late }
    }
}

/// Guard: `ts` must end up non-decreasing or the pipeline contract broke.
fn keep_runs(ts: &[i64], ctx: &'static str) {
    if ts.windows(2).any(|w| w[0] > w[1]) {
        panic!("{ctx}: ts not sorted after stable sort — engine bug");
    }
}
