//! `.mkt` round-trip + corruption tests (Phase 2 acceptance, spec §7).
//!
//! Fixtures are deliberately small and multi-chunk (2_341 rows, chunk=1_000 →
//! 3 chunks: 1000/1000/341) so chunk-boundary alignment is exercised on every run.

use std::path::PathBuf;

use tf_format::error::MktError;
use tf_format::{
    write_mkt_chunked, Codec, ColData, ColumnIn, MktFile,
    COL_FLAGS, COL_PRICE, COL_QTY, COL_QUOTE_QTY, COL_TRADE_ID, COL_TS,
    FLAG_IS_BEST_MATCH, FLAG_IS_BUYER_MAKER,
};

const ROWS: usize = 2_341;
const CHUNK: u32 = 1_000;

fn fixture() -> Vec<ColumnIn> {
    let n = ROWS;
    let mut trade_id = Vec::with_capacity(n);
    let mut price = Vec::with_capacity(n);
    let mut qty = Vec::with_capacity(n);
    let mut quote = Vec::with_capacity(n);
    let mut ts = Vec::with_capacity(n);
    let mut flags = Vec::with_capacity(n);
    let base_ts: i64 = 1_793_580_000_000_000; // µs around 2026-10-31
    for i in 0..n {
        let p = 111_000.5 + i as f64 * 0.25;
        let q = 0.001 * (i % 100 + 1) as f64;
        trade_id.push(42 + i as i64);
        price.push(p);
        qty.push(q);
        quote.push(p * q);
        ts.push(base_ts + i as i64 * 137);
        flags.push(if i % 3 == 0 {
            FLAG_IS_BUYER_MAKER
        } else if i % 7 == 0 {
            FLAG_IS_BEST_MATCH
        } else {
            FLAG_IS_BUYER_MAKER | FLAG_IS_BEST_MATCH
        });
    }
    vec![
        ColumnIn { id: COL_TRADE_ID, codec: Codec::Zstd, data: ColData::I64(trade_id) },
        ColumnIn { id: COL_PRICE, codec: Codec::Zstd, data: ColData::F64(price) },
        ColumnIn { id: COL_QTY, codec: Codec::Zstd, data: ColData::F64(qty) },
        ColumnIn { id: COL_QUOTE_QTY, codec: Codec::Zstd, data: ColData::F64(quote) },
        ColumnIn { id: COL_TS, codec: Codec::Zstd, data: ColData::I64(ts) },
        ColumnIn { id: COL_FLAGS, codec: Codec::Zstd, data: ColData::U8(flags) },
    ]
}

fn out_path(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join("tfmk_tests");
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(name)
}

#[test]
fn roundtrip_multi_chunk_bitwise() {
    let path = out_path("roundtrip.mkt");
    let cols = fixture();
    let stats = write_mkt_chunked(&path, "BTCUSDT", cols.clone(), CHUNK).unwrap();

    assert_eq!(stats.rows, ROWS as u64);
    assert_eq!(stats.chunks, 3, "chunk alignment must split 2341 into 3 chunks");

    let f = MktFile::open(&path).unwrap();
    assert_eq!(f.meta.symbol, "BTCUSDT");
    assert_eq!(f.meta.row_count, ROWS as u64);
    assert_eq!(f.meta.chunk_rows_max, CHUNK);
    assert_eq!(f.meta.schema_version, tf_core::schema::SCHEMA_VERSION);

    // file-level ts bounds == fixture bounds
    let want_ts = match &cols[4].data {
        ColData::I64(v) => (*v.first().unwrap(), *v.last().unwrap()),
        _ => panic!(),
    };
    assert_eq!((f.meta.ts_min, f.meta.ts_max), want_ts);

    // every column, every row, bitwise
    for col in &cols {
        let got = f.decode_column(col.id).unwrap();
        assert_eq!(got, col.data, "column {} mismatch", col.id);
    }

    // per-chunk decode matches the aligned ranges
    assert_eq!(f.n_chunks(COL_TS).unwrap(), 3);
    assert_eq!(f.decode_chunk(COL_TS, 0).unwrap().len(), 1_000);
    assert_eq!(f.decode_chunk(COL_TS, 1).unwrap().len(), 1_000);
    assert_eq!(f.decode_chunk(COL_TS, 2).unwrap().len(), ROWS - 2_000);
}

#[test]
fn ts_range_pruning_skips_chunks_by_minmax_only() {
    let path = out_path("pruning.mkt");
    write_mkt_chunked(&path, "BTCUSDT", fixture(), CHUNK).unwrap();
    let f = MktFile::open(&path).unwrap();
    let fixture_ts = match &fixture()[4].data {
        ColData::I64(v) => v.clone(),
        _ => panic!(),
    };

    // window inside chunk 1 only → chunks {1}, not 0 or 2
    let t1 = fixture_ts[1_300];
    let t2 = fixture_ts[1_400];
    assert_eq!(f.chunks_for_ts_range(t1, t2).unwrap(), vec![1]);

    // window touching chunks 0 and 1 but not 2
    let t1 = fixture_ts[900];
    let t2 = fixture_ts[1_100];
    assert_eq!(f.chunks_for_ts_range(t1, t2).unwrap(), vec![0, 1]);

    // empty window → no chunks touched
    let t0 = fixture_ts[0] - 1_000_000;
    assert!(f.chunks_for_ts_range(t0, t0 - 1).unwrap().is_empty());
}

#[test]
fn rewrite_is_byte_identical_deterministic() {
    let path = out_path("determinism.mkt");
    let bytes1 = {
        write_mkt_chunked(&path, "BTCUSDT", fixture(), CHUNK).unwrap();
        std::fs::read(&path).unwrap()
    };
    let bytes2 = {
        write_mkt_chunked(&path, "BTCUSDT", fixture(), CHUNK).unwrap();
        std::fs::read(&path).unwrap()
    };
    assert_eq!(bytes1, bytes2, "zstd output must be deterministic for identical input");
    // and the .tmp is gone after the atomic rename
    assert!(!path.with_extension("mkt.tmp").exists());
}

#[test]
fn corrupted_trailing_magic_rejected() {
    let path = out_path("corrupt_tail.mkt");
    write_mkt_chunked(&path, "BTCUSDT", fixture(), CHUNK).unwrap();
    let mut bytes = std::fs::read(&path).unwrap();
    let n = bytes.len();
    bytes[n - 1] ^= 0xFF; // flip last magic byte
    std::fs::write(&path, &bytes).unwrap();
    let err = MktFile::open(&path).unwrap_err();
    assert!(matches!(err, MktError::BadMagic(_)), "got: {err}");
}

#[test]
fn truncated_footer_rejected() {
    let path = out_path("trunc.mkt");
    write_mkt_chunked(&path, "BTCUSDT", fixture(), CHUNK).unwrap();
    let bytes = std::fs::read(&path).unwrap();
    std::fs::write(&path, &bytes[..bytes.len() / 2]).unwrap();
    let err = MktFile::open(&path).unwrap_err();
    // footer tail magic no longer at EOF (or body truncated) — must fail loudly
    assert!(
        matches!(err, MktError::BadMagic(_) | MktError::FooterTruncated { .. }),
        "got: {err}"
    );
}

#[test]
fn footer_data_corruption_detected_on_decode() {
    // index bounds survive, but flipping bits inside a payload must not decode
    // successfully into a shorter/other-length vec without an error
    let path = out_path("corrupt_payload.mkt");
    write_mkt_chunked(&path, "BTCUSDT", fixture(), CHUNK).unwrap();
    let mut bytes = std::fs::read(&path).unwrap();
    for b in bytes.iter_mut().take(2_000).skip(1_000) {
        *b ^= 0xFF; // corrupt middle of chunk 0 payloads
    }
    std::fs::write(&path, &bytes).unwrap();
    let f = MktFile::open(&path).unwrap(); // footer still valid
    // at least one of the payload decodes must fail (zstd CRC check)
    let some_err = (0..6)
        .map(|id| f.decode_chunk(id, 0))
        .any(|r| r.is_err());
    assert!(some_err, "corrupted payloads must fail loudly on decode");
}

#[test]
fn plain_codec_roundtrip() {
    // one column Plain, rest Zstd — per-column codec must be honored
    let path = out_path("plain_codec.mkt");
    let mut cols = fixture();
    cols[5].codec = Codec::Plain;
    write_mkt_chunked(&path, "BTCUSDT", cols.clone(), CHUNK).unwrap();
    let f = MktFile::open(&path).unwrap();
    assert_eq!(f.decode_column(COL_FLAGS).unwrap(), cols[5].data);
}
