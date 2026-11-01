//! Canonical tick schema — SINGLE SOURCE OF TRUTH (Rust side).
//!
//! Python mirror: `python/tickforge/schema.py` — keep them field-for-field identical.
//! Any change here must be mirrored there (and vice versa) in the same commit.
//!
//! Contract source: binance-public-data spot aggTrades CSV (headerless, 8 columns);
//! spot timestamps from 2025-01-01 onwards are MICROSECONDS (earlier files: milliseconds).

/// Bumped on any schema field addition/removal/type change.
pub const SCHEMA_VERSION: u32 = 1;

/// Event-time unit cutoff: spot file `ts` are microseconds for dates >= 2025-01-01, else milliseconds.
pub const TS_MICROSECOND_CUTOFF: &str = "2025-01-01";

/// One canonical aggregated-trade row, exactly matching `CANONICAL_FIELDS` in `schema.py`.
#[derive(Debug, Clone, PartialEq)]
pub struct AggTrade {
    /// aggregated trade id (u64 in the CSV; within i64 range per symbol/day)
    pub trade_id: i64,
    /// e.g. "BTCUSDT" (uppercased)
    pub symbol: String,
    /// aggregated price — all merged trades share one price
    pub price: f64,
    /// summed base quantity of the aggregate
    pub qty: f64,
    /// DERIVED = price * qty (exact for aggTrades; see ADR note in schema.py)
    pub quote_qty: f64,
    /// event time in MICROSECONDS since epoch (canonical unit)
    pub ts: i64,
    /// true = the buy side was the resting (maker) order
    pub is_buyer_maker: bool,
    /// was the trade the best price match
    pub is_best_match: bool,
}

impl AggTrade {
    /// Field order must match the canonical tuple in `schema.py`.
    pub const FIELD_COUNT: usize = 8;
}

/// `'us'` for dates on/after the cutoff, else `'ms'` (caller must convert *1000).
pub fn ts_unit_for_date(date: &str) -> &'static str {
    if date >= TS_MICROSECOND_CUTOFF {
        "us"
    } else {
        "ms"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_row_roundtrip_values() {
        let t = AggTrade {
            trade_id: 42,
            symbol: "BTCUSDT".into(),
            price: 0.2,
            qty: 50.0,
            quote_qty: 0.2 * 50.0,
            ts: 1_735_689_600_010_866,
            is_buyer_maker: false,
            is_best_match: true,
        };
        assert_eq!(t.trade_id, 42);
        assert_eq!(t.ts, 1_735_689_600_010_866); // 2025-01-01T00:00:00.001086Z in µs
        assert_eq!(t.quote_qty, 10.0);
        assert_eq!(AggTrade::FIELD_COUNT, 8);
    }

    #[test]
    fn ts_unit_cutoff_is_us_for_2025_spot_data() {
        assert_eq!(ts_unit_for_date("2025-01-01"), "us");
        assert_eq!(ts_unit_for_date("2026-10-31"), "us");
        assert_eq!(ts_unit_for_date("2024-12-31"), "ms"); // legacy: convert *1000
    }
}
