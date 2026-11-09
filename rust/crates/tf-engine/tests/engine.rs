//! tf-engine tests (plan.md Phase 3): scan pruning, resample correctness,
//! dedup (Drill 3) and out-of-order/late handling (Drill 4) — all on small
//! multi-chunk fixtures so boundary arithmetic is exercised every run.

use tf_engine::{RowTable, dedup, resample, scan_ts_range};
use tf_format::{Codec, ColData, ColumnIn, MktFile, FLAG_IS_BUYER_MAKER};

const CHUNK: u32 = 100;

/// Build a canonical-ish fixture: n rows, ts ascending with `ts_step` µs steps,
/// starting at `base_ts`.
fn fixture_rows(base_ts: i64, ts_step: i64, n: usize) -> RowTable {
    let mut rows = RowTable::default();
    for i in 0..n {
        let p = 111_000.25 + i as f64 * 0.5;
        let q = 0.001 * ((i % 50 + 1) as f64);
        rows.trade_id.push(1000 + i as i64);
        rows.ts.push(base_ts + (i as i64) * ts_step);
        rows.price.push(p);
        rows.qty.push(q);
        rows.quote_qty.push(p * q);
        rows.flags.push(if i % 4 == 0 { FLAG_IS_BUYER_MAKER } else { 0 });
    }
    rows
}

fn to_columns(rows: &RowTable) -> Vec<ColumnIn> {
    vec![
        ColumnIn { id: tf_format::COL_TRADE_ID, codec: Codec::Zstd, data: ColData::I64(rows.trade_id.clone()) },
        ColumnIn { id: tf_format::COL_PRICE, codec: Codec::Zstd, data: ColData::F64(rows.price.clone()) },
        ColumnIn { id: tf_format::COL_QTY, codec: Codec::Zstd, data: ColData::F64(rows.qty.clone()) },
        ColumnIn { id: tf_format::COL_QUOTE_QTY, codec: Codec::Zstd, data: ColData::F64(rows.quote_qty.clone()) },
        ColumnIn { id: tf_format::COL_TS, codec: Codec::Zstd, data: ColData::I64(rows.ts.clone()) },
        ColumnIn { id: tf_format::COL_FLAGS, codec: Codec::Zstd, data: ColData::U8(rows.flags.clone()) },
    ]
}

fn write_then_open(path: &str, rows: &RowTable) -> MktFile {
    let p = std::env::temp_dir().join("tfeng_tests").join(path);
    std::fs::create_dir_all(p.parent().unwrap()).unwrap();
    tf_format::write_mkt_chunked(&p, "BTCUSDT", to_columns(rows), CHUNK).unwrap();
    MktFile::open(&p).unwrap()
}

// ---- scan --------------------------------------------------------------------

#[test]
fn scan_emits_exactly_the_window_rows() {
    let base: i64 = 1_793_548_800_000_000; // 2026-10-31T12:00:00Z-ish
    let rows = fixture_rows(base, 137, 1_000); // 137µs steps, 3 chunks
    let f = write_then_open("scan_exact.mkt", &rows);

    // window = rows [250, 750) → exact expected row list from the source arrays
    let t1 = rows.ts[250];
    let t2 = rows.ts[749];
    let (sc, st) = scan_ts_range(&f, t1, t2).unwrap();
    assert_eq!(st.rows_emitted, 500);
    assert_eq!(sc.ts.first(), Some(&t1));
    assert_eq!(sc.ts.last(), Some(&t2));
    // and the actual values match the fixture verbatim
    for (j, i) in (250..750).enumerate() {
        assert_eq!(sc.trade_id[j], rows.trade_id[i]);
        assert_eq!(sc.price[j], rows.price[i]);
        assert_eq!(sc.qty[j], rows.qty[i]);
        assert_eq!(sc.flags[j], rows.flags[i]);
    }
}

#[test]
fn scan_prunes_chunks_by_minmax() {
    let base: i64 = 1_793_548_800_000_000;
    let rows = fixture_rows(base, 1_000_137, 1_000); // ≥1s steps → 1/2 chunks per window
    let f = write_then_open("scan_prune.mkt", &rows);
    assert_eq!(f.n_chunks(tf_format::COL_TS).unwrap(), 10, "1000 rows / chunk=100");

    // window strictly inside chunk 5's rows → exactly 1 scanned, 9 skipped
    let t1 = rows.ts[550];
    let t2 = rows.ts[590];
    let (sc, st) = scan_ts_range(&f, t1, t2).unwrap();
    assert_eq!(st.rows_emitted, 41);
    assert_eq!(st.chunks_scanned, 1);
    assert_eq!(st.chunks_skipped, 9);

    // window entirely before or after the data → nothing is even decoded
    let (sc, st) = scan_ts_range(&f, rows.ts[0] - 10_000_000, rows.ts[0] - 1_000_000).unwrap();
    assert_eq!(sc.row_count(), 0);
    assert_eq!(st.chunks_scanned, 0);
    assert_eq!(st.chunks_skipped, 10);
    assert_eq!(st.rows_decoded, 0);
}

// ---- resample ----------------------------------------------------------------

#[test]
fn resample_1s_matches_reference_bars() {
    // rows inside first two seconds stepping 250ms → 4 trades/bar; prices vary
    let base: i64 = 1_793_548_800_000_000;
    let rows = fixture_rows(base, 250_000, 8); // 2 buckets: rows 0..4, 4..8
    let sc = rows_as_scanned(&rows);
    let bars = resample(&sc, 1);
    assert_eq!(bars.len(), 2);

    let b0 = &bars[0];
    assert_eq!(b0.bucket_start_us, base - base.rem_euclid(1_000_000));
    assert_eq!(b0.trades, 4);
    assert_eq!(b0.open, rows.price[0]);
    assert_eq!(b0.close, rows.price[3]);
    assert_eq!(b0.high, rows.price[0].max(rows.price[3]));
    assert_eq!(b0.low, rows.price[0].min(rows.price[3]));
    let vol0: f64 = rows.qty[0..4].iter().sum();
    assert_eq!(b0.base_vol, vol0);
    // flags: rows 0,4 → maker (aggressor=SELL); rows 1..3 buy
    assert_eq!(b0.sell_base, rows.qty[0]);
    assert_eq!(b0.buy_base, rows.qty[1] + rows.qty[2] + rows.qty[3]);
    assert_eq!(bars[1].trades, 4);
    assert_eq!(bars[1].close, rows.price[7]);
}

/// Drill 4 (late/out-of-order) at engine level: shuffled input is sorted, the
/// late counter jumps, and the late event lands in its CORRECT 1s bucket.
#[test]
fn late_event_lands_in_correct_bar_after_sort() {
    let sec: i64 = 1_000_000;
    let base: i64 = 1_793_548_800_000_000;
    let mut rows = fixture_rows(base + 2 * sec, 300_000, 2); // rows at t=+2.0s, +2.3s
    // inject a LATE trade for t=+0.5s AT THE END of the stream (arrives after
    // two events with strictly newer timestamps — that's what "late" means)
    rows.trade_id.push(999_001);
    rows.ts.push(base + 500_000);
    rows.price.push(111_500.0);
    rows.qty.push(2.5);
    rows.quote_qty.push(111_500.0 * 2.5);
    rows.flags.push(0);

    let order = rows.sort_row_ts();
    assert_eq!(order.late_events, 1);
    assert_eq!(rows.ts[0], base + 500_000, "late row sorted first");
    assert_eq!(rows.ts[1], base + 2 * sec);

    let sc = rows_as_scanned(&rows);
    let bars = resample(&sc, 1);
    // bucket 1: only the late event; buckets 2,3: the two +2s rows split
    let b_late = bars[0];
    assert_eq!(b_late.bucket_start_us, base);
    assert_eq!(b_late.trades, 1);
    assert_eq!(b_late.open, 111_500.0);
}

// ---- dedup (Drill 3) ---------------------------------------------------------

#[test]
fn dedup_five_identical_events() {
    let base: i64 = 1_793_548_800_000_000;
    let mut rows = RowTable::default();
    for _ in 0..5 {
        rows.trade_id.push(777);
        rows.ts.push(base);
        rows.price.push(111_000.25);
        rows.qty.push(0.5);
        rows.quote_qty.push(111_000.25 * 0.5);
        rows.flags.push(0);
    }
    let (st, kept) = dedup(&rows).unwrap();
    assert_eq!(st.inserted, 1, "one identity row, inserted exactly once");
    assert_eq!(st.duplicates, 4, "the other 4 are exact duplicates");
    assert_eq!(kept.len(), 1);
    assert_eq!(kept.ts, vec![base]);
}

#[test]
fn dedup_collision_with_different_payload_fails_loudly() {
    let base: i64 = 1_793_548_800_000_000;
    let mut rows = RowTable::default();
    rows.trade_id = vec![7, 7];
    rows.ts = vec![base, base];
    rows.price = vec![111.0, 111.5]; // same id, DIFFERENT price → conflict
    rows.qty = vec![1.0, 1.0];
    rows.quote_qty = vec![111.0, 111.5];
    rows.flags = vec![0, 0];
    assert!(dedup(&rows).is_err(), "same trade_id + different payload is a contract violation");
}

// ---- helper ------------------------------------------------------------------

fn rows_as_scanned(rows: &RowTable) -> tf_engine::Scanned {
    tf_engine::Scanned {
        ts: rows.ts.clone(),
        trade_id: rows.trade_id.clone(),
        price: rows.price.clone(),
        qty: rows.qty.clone(),
        quote_qty: rows.quote_qty.clone(),
        flags: rows.flags.clone(),
    }
}
