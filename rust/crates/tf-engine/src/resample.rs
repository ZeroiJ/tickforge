//! OHLCV resampling — trades → bars (plan.md Phase 3 "Resample").
//!
//! Buckets are aligned to epoch: `bucket = ts - ts % (bucket_secs*1e6)` in canonical µs.
//! Events must be in time order before resampling (pipeline::sort_rows_by_ts in the
//! ingest path guarantees that for anything written to `.mkt`).
//!
//! Bar fields follow standard OHLCV + taker-side split (binance semantics):
//! `is_buyer_maker = true` → the buyer order was resting → the AGGRESSOR was the
//! seller, so qty with that flag counts as `sell_base`/`sell_quote`; the rest is the
//! taker-buy side. This split is what Phase 4 (R orderflow research) consumes.

use crate::scan::Scanned;
use crate::US_PER_SEC;

/// One aggregated bar over `[bucket_start_us, bucket_start_us + bucket_us)`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Bar {
    pub bucket_start_us: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub base_vol: f64,
    pub quote_vol: f64,
    pub trades: u64,
    /// aggressor-buy volume (is_buyer_maker == false)
    pub buy_base: f64,
    /// aggressor-sell volume (is_buyer_maker == true)
    pub sell_base: f64,
}

/// Resample scanned rows into `bucket_secs` bars, ASC ordered, only non-empty
/// buckets emitted. Empty buckets (gaps) are intentionally absent — a real gap
/// is data, not noise (microstructure drill material for Phase 4).
pub fn resample(sc: &Scanned, bucket_secs: u64) -> Vec<Bar> {
    let bucket_us = bucket_secs.max(1) as i64 * US_PER_SEC;
    let mut bars: Vec<Bar> = Vec::new();

    for i in 0..sc.row_count() {
        let bucket = sc.ts[i] - (sc.ts[i].rem_euclid(bucket_us));
        let (buy, sell) = if sc.flags[i] & tf_format::FLAG_IS_BUYER_MAKER != 0 {
            (0.0, sc.qty[i])
        } else {
            (sc.qty[i], 0.0)
        };

        let last = if bars.last().map_or(false, |b| b.bucket_start_us == bucket) {
            let b = bars.last_mut().unwrap();
            b.high = b.high.max(sc.price[i]);
            b.low = b.low.min(sc.price[i]);
            b.close = sc.price[i];
            b.base_vol += sc.qty[i];
            b.quote_vol += sc.quote_qty[i];
            b.trades += 1;
            b.buy_base += buy;
            b.sell_base += sell;
            None
        } else {
            Some(Bar {
                bucket_start_us: bucket,
                open: sc.price[i],
                high: sc.price[i],
                low: sc.price[i],
                close: sc.price[i],
                base_vol: sc.qty[i],
                quote_vol: sc.quote_qty[i],
                trades: 1,
                buy_base: buy,
                sell_base: sell,
            })
        };
        if let Some(b) = last {
            bars.push(b);
        }
    }
    bars
}
