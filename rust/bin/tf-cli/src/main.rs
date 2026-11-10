//! `tf-cli` â€” the single tickforge binary.
//!
//! Phase 2 surface (plan.md):
//! - `ingest-norm`  : normalized canonical Parquet â†’ `.mkt` (atomic, idempotent, sha-keyed)
//! - `roundtrip`    : decode `.mkt` back and compare to Parquet values bitwise (spec Â§7 proof)
//! Phase 3 surface (plan.md):
//! - `query`        : time-range scan on `.mkt` (chunk pruning) + optional OHLCV
//!                    resample â†’ bars parquet (Drills 5/6 material)

use std::collections::BTreeSet;
use std::error::Error;
use std::fs::File;
use std::path::{Path, PathBuf};

use arrow_array::{
    cast::AsArray,
    types::{Float64Type, Int64Type},
    Array,
};
use sha2::Digest;
use clap::{Parser, Subcommand};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use tf_engine::RowTable;
use tf_format::{
    write_mkt, write_mkt_chunked, Codec, ColData, ColumnIn, MktFile,
    COL_FLAGS, COL_PRICE, COL_QTY, COL_QUOTE_QTY, COL_TRADE_ID, COL_TS,
    FLAG_IS_BEST_MATCH, FLAG_IS_BUYER_MAKER,
};

#[derive(Parser)]
#[command(name = "tf-cli", version, about = "tickforge CLI")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Normalized canonical Parquet -> .mkt (idempotent by source sha256).
    IngestNorm {
        /// Input parquet, e.g. data/norm/btcusdt/dt=2026-10-31/trades.parquet
        #[arg(short, long)]
        input: PathBuf,
        /// Output root. Default: <repo>/data/mkt.
        #[arg(short, long)]
        out: Option<PathBuf>,
        /// Use this chunk size instead of 10^6 rows (tests/benches).
        #[arg(long)]
        chunk_rows: Option<u32>,
    },
    /// Decode .mkt and compare every value against the Parquet source, bitwise.
    Roundtrip {
        /// The .mkt file. Default: data/mkt/<symbol>/dt=<date>/trades.mkt.
        #[arg(short, long)]
        mkt: Option<PathBuf>,
        /// Input parquet to compare against.
        #[arg(short, long)]
        input: PathBuf,
    },
    /// Time-range scan + optional OHLCV resample over one or more `.mkt` files
    /// (Phase 3; multi-file = Drill 5's 24h/5-symbol shape).
    Query {
        /// The .mkt file(s) to scan (repeat the flag for multiple files).
        #[arg(short, long = "mkt")]
        mkt: Vec<PathBuf>,
        /// Window start, inclusive, canonical microseconds.
        #[arg(long)]
        from: i64,
        /// Window end, inclusive, canonical microseconds.
        #[arg(long)]
        to: i64,
        /// Optional resample bucket SECONDS (e.g. 1 = 1s bars).
        #[arg(long)]
        resample: Option<u64>,
        /// When resampling: write bars here as parquet (e.g. bars_1s.parquet).
        #[arg(short, long)]
        out: Option<PathBuf>,
    },
    /// Stream events from disk in time order (Phase 3 replay / Drill 6).
    ///
    /// `--speed N` paces the stream NÃ— real time (event-d.UTC spacing / N);
    /// omit (`--speed 0`) for max-rate integrity mode: the streamed path
    /// aggregates bars incrementally and they must equal `query --resample`s
    /// full-pass bars exactly.
    Replay {
        /// The .mkt file to replay.
        #[arg(short, long)]
        mkt: PathBuf,
        /// Replay window start, inclusive, canonical microseconds.
        #[arg(long)]
        from: i64,
        /// Replay window end, inclusive, canonical microseconds.
        #[arg(long)]
        to: i64,
        /// Replay speed multiplier (0 = max-rate integrity run).
        #[arg(long)]
        speed: Option<f64>,
        /// Optional resample bucket SECONDS while streaming (Drill 6 bars).
        #[arg(long)]
        resample: Option<u64>,
        /// When resampling: write the streamed bars here as parquet.
        #[arg(short, long)]
        out: Option<PathBuf>,
    },
}

fn sha256_hex(path: &Path) -> Result<String, Box<dyn Error>> {
    let mut hasher = sha2::Sha256::new();
    let mut f = File::open(path)?;
    std::io::copy(&mut f, &mut hasher)?;
    Ok(format!("{:x}", hasher.finalize()))
}

/// Repository root: CARGO_MANIFEST_DIR = <repo>/rust/bin/tf-cli.
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3) // <repo>/rust/bin/tf-cli -> <repo>
        .expect("manifest below workspace root")
        .to_path_buf()
}

/// Canonical layout info inferred from `<...>/<symbol-lower>/dt=<date>/trades.parquet`.
fn infer_symbol_date(input: &Path) -> Result<(String, String), Box<dyn Error>> {
    let dir = input
        .parent()
        .ok_or("input parquet has no parent directory")?;
    let date = dir
        .file_name()
        .and_then(|n| n.to_str())
        .and_then(|n| n.strip_prefix("dt="))
        .ok_or("cannot infer dt=<date> from input path")?
        .to_string();
    let sym = dir
        .parent()
        .and_then(|d| d.file_name())
        .and_then(|n| n.to_str())
        .ok_or("cannot infer symbol from input path")?
        .to_string();
    Ok((sym, date))
}

/// Reads canonical Parquet into typed columns (field order = tickforge.schema).
struct Canonical {
    symbol: String,
    trade_id: Vec<i64>,
    price: Vec<f64>,
    qty: Vec<f64>,
    quote_qty: Vec<f64>,
    ts: Vec<i64>,
    flags: Vec<u8>,
}

impl Canonical {
    fn read(parquet: &Path) -> Result<Self, Box<dyn Error>> {
        let file = File::open(parquet)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;

        let mut symbols: BTreeSet<String> = BTreeSet::new();
        let (mut trade_id, mut price, mut qty, mut quote_qty, mut ts, mut flags) =
            (vec![], vec![], vec![], vec![], vec![], vec![]);

        for batch in reader {
            let b = batch?;
            let col_id = |name: &str| {
                b.schema().index_of(name)
                    .map_err(|e| format!("missing canonical column '{name}' (contract: tickforge/schema.py): {e}"))
            };
            let trade = b.column(col_id("trade_id")?).as_primitive::<Int64Type>();
            let price_arr = b.column(col_id("price")?).as_primitive::<Float64Type>();
            let qty_arr = b.column(col_id("qty")?).as_primitive::<Float64Type>();
            let quote_arr = b.column(col_id("quote_qty")?).as_primitive::<Float64Type>();
            let ts_arr = b.column(col_id("ts")?).as_primitive::<Int64Type>();
            let bm = b.column(col_id("is_buyer_maker")?).as_boolean();
            let best = b.column(col_id("is_best_match")?).as_boolean();
            // polars writes `symbol` as large_string; accept that exactly
            let sym = b.column(col_id("symbol")?).as_string::<i64>();

            for j in 0..b.num_rows() {
                if trade.is_null(j) || ts_arr.is_null(j) || price_arr.is_null(j) || qty_arr.is_null(j) {
                    return Err(format!("null in key column at row {} â€” contract violation", trade_id.len()).into());
                }
                if !bm.is_null(j) && !best.is_null(j) {
                    let mut f = 0u8;
                    if bm.value(j) {
                        f |= FLAG_IS_BUYER_MAKER;
                    }
                    if best.value(j) {
                        f |= FLAG_IS_BEST_MATCH;
                    }
                    flags.push(f);
                }
                trade_id.push(trade.value(j));
                price.push(price_arr.value(j));
                qty.push(qty_arr.value(j));
                quote_qty.push(quote_arr.value(j));
                ts.push(ts_arr.value(j));
                if !sym.is_null(j) {
                    symbols.insert(sym.value(j).to_string());
                }
            }
        }
        if symbols.len() > 1 {
            return Err(format!(
                "file contains multiple symbols {symbols:?} â€” one .mkt file per (symbol, day)"
            ).into());
        }
        let symbol = symbols.pop_first().ok_or("empty input")?;
        Ok(Self { symbol, trade_id, price, qty, quote_qty, ts, flags })
    }

    fn columns(&self) -> Vec<ColumnIn> {
        vec![
            ColumnIn { id: COL_TRADE_ID, codec: Codec::Zstd, data: ColData::I64(self.trade_id.clone()) },
            ColumnIn { id: COL_PRICE, codec: Codec::Zstd, data: ColData::F64(self.price.clone()) },
            ColumnIn { id: COL_QTY, codec: Codec::Zstd, data: ColData::F64(self.qty.clone()) },
            ColumnIn { id: COL_QUOTE_QTY, codec: Codec::Zstd, data: ColData::F64(self.quote_qty.clone()) },
            ColumnIn { id: COL_TS, codec: Codec::Zstd, data: ColData::I64(self.ts.clone()) },
            ColumnIn { id: COL_FLAGS, codec: Codec::Zstd, data: ColData::U8(self.flags.clone()) },
        ]
    }
}

fn ingest_norm(input: PathBuf, out: Option<PathBuf>, chunk_rows: Option<u32>) -> Result<(), Box<dyn Error>> {
    let input = std::fs::canonicalize(&input)?;
    let out_root = out.unwrap_or_else(|| repo_root().join("data").join("mkt"));
    let (_, date) = infer_symbol_date(&input)?;

    let sha = sha256_hex(&input)?;
    let mut canon = Canonical::read(&input)?;
    let symbol_lower = canon.symbol.to_lowercase();

    let out_dir = out_root.join(&symbol_lower).join(format!("dt={date}"));
    let mkt_path = out_dir.join("trades.mkt");
    let sidecar = out_dir.join("stats.json");
    std::fs::create_dir_all(&out_dir)?;

    // idempotency mirroring normalize.py: matching source sha -> written=0
    if mkt_path.exists() && sidecar.exists() {
        let prev: serde_json::Value = serde_json::from_reader(File::open(&sidecar)?)?;
        if prev.get("source_sha256") == Some(&serde_json::Value::String(sha.clone())) {
            println!(
                "skipped (source sha256 match, existing .mkt intact): rows_written=0 out_path={}",
                mkt_path.display()
            );
            return Ok(());
        }
    }

    // ---- Phase 3 gate: dedup + time-ordering (Drills 3 & 4, ADR-0002) -------
    // On day-scale data (â‰¤ ~1M rows/symbol-day) the row-table build + sort is
    // memory-trivial and keeps the `.mkt` write path time-ascending by contract.
    let mut rows = RowTable {
        trade_id: canon.trade_id.clone(),
        ts: canon.ts.clone(),
        price: canon.price.clone(),
        qty: canon.qty.clone(),
        quote_qty: canon.quote_qty.clone(),
        flags: canon.flags.clone(),
    };
    let (dup, kept) = tf_engine::dedup(&rows)?;
    rows = kept;
    let ord = rows.sort_row_ts();
    canon.trade_id = rows.trade_id;
    canon.ts = rows.ts;
    canon.price = rows.price;
    canon.qty = rows.qty;
    canon.quote_qty = rows.quote_qty;
    canon.flags = rows.flags;

    let cols = canon.columns();
    let stats = match chunk_rows {
        Some(cr) => write_mkt_chunked(&mkt_path, &canon.symbol, cols, cr)?,
        None => write_mkt(&mkt_path, &canon.symbol, cols)?,
    };

    let side = serde_json::json!({
        "source_sha256": sha,
        "source": input.display().to_string(),
        "symbol": canon.symbol,
        "date": date,
        "rows": stats.rows,
        "chunks": stats.chunks,
        "ts_min": stats.ts_min,
        "ts_max": stats.ts_max,
        "mkt_bytes": stats.file_bytes,
        "uncomp_bytes": stats.uncomp_bytes,
        "duplicates_skipped": dup.duplicates,
        "late_events": ord.late_events,
        "format": "mkt-v1",
        "spec": "docs/format-spec.md",
    });
    serde_json::to_writer_pretty(File::create(&sidecar)?, &side)?;

    println!(
        "rows_written={} duplicates_skipped={} late_events={} chunks={} mkt_bytes={} uncomp_bytes={} out_path={}",
        stats.rows, dup.duplicates, ord.late_events, stats.chunks, stats.file_bytes, stats.uncomp_bytes, mkt_path.display()
    );
    Ok(())
}

fn roundtrip(mkt: Option<PathBuf>, input: PathBuf) -> Result<(), Box<dyn Error>> {
    let input = std::fs::canonicalize(&input)?;
    let canon = Canonical::read(&input)?;

    let path = match mkt {
        Some(p) => p,
        None => {
            let (_, date) = infer_symbol_date(&input)?;
            repo_root().join("data").join("mkt")
                .join(canon.symbol.to_lowercase())
                .join(format!("dt={date}"))
                .join("trades.mkt")
        }
    };

    let f = MktFile::open(&path)?;
    let rows_total = f.meta.row_count;
    let mut mismatches = 0usize;

    for chunk in 0..f.n_chunks(COL_TS)? {
        let s = chunk_start(&f, COL_TS, chunk) as usize;

        // integer columns compare exactly
        let got = expected_i64(&f, COL_TRADE_ID, chunk)?;
        mismatches += got.iter().zip(&canon.trade_id[s..s + got.len()]).filter(|(g, w)| g != w).count();
        let got = expected_i64(&f, COL_TS, chunk)?;
        mismatches += got.iter().zip(&canon.ts[s..s + got.len()]).filter(|(g, w)| g != w).count();

        // f64 compare bitwise (u-bits), zero tolerance
        for (col_id, want) in [
            (COL_PRICE, &canon.price),
            (COL_QTY, &canon.qty),
            (COL_QUOTE_QTY, &canon.quote_qty),
        ] {
            let got = expected_f64(&f, col_id, chunk)?;
            mismatches += got
                .iter()
                .zip(&want[s..s + got.len()])
                .filter(|(g, w)| g.to_bits() != w.to_bits())
                .count();
        }

        // packed flags column decodes to the same u8 bytes
        let got = expected_u8(&f, COL_FLAGS, chunk)?;
        mismatches += got.iter().zip(&canon.flags[s..s + got.len()]).filter(|(g, w)| g != w).count();
    }

    let mkt_bytes = std::fs::metadata(&path)?.len();
    let parquet_bytes = std::fs::metadata(&input)?.len();

    // optional context: original raw csv bytes, when present at data/raw/
    let mut csv_note = String::new();
    let date_opt = infer_symbol_date(&input).ok().map(|(_, d)| d);
    if let Some(date) = &date_opt {
        let csv = repo_root().join("data").join("raw").join(format!(
            "{}-aggTrades-{}.csv",
            canon.symbol.to_uppercase(),
            date
        ));
        if let Ok(md) = std::fs::metadata(&csv) {
            csv_note = format!(
                " csv_bytes={} mkt/csv_ratio={:.1}x",
                md.len(),
                md.len() as f64 / mkt_bytes as f64
            );
        }
    }

    println!(
        "rows={} {} mkt_bytes={} parquet_bytes={} parquet/mkt={:.2}{}",
        rows_total,
        if mismatches == 0 { "ALL-BITWISE-MATCH" } else { "MISMATCHES" },
        mkt_bytes,
        parquet_bytes,
        parquet_bytes as f64 / mkt_bytes as f64,
        csv_note,
    );
    if mismatches != 0 {
        return Err(format!("roundtrip FAILED: {mismatches} mismatches").into());
    }
    Ok(())
}

fn expected_i64(f: &MktFile, col_id: u8, chunk: usize) -> Result<Vec<i64>, Box<dyn Error>> {
    match f.decode_chunk(col_id, chunk)? {
        ColData::I64(v) => Ok(v),
        _ => Err("column type mismatch".into()),
    }
}
fn expected_f64(f: &MktFile, col_id: u8, chunk: usize) -> Result<Vec<f64>, Box<dyn Error>> {
    match f.decode_chunk(col_id, chunk)? {
        ColData::F64(v) => Ok(v),
        _ => Err("column type mismatch".into()),
    }
}
fn expected_u8(f: &MktFile, col_id: u8, chunk: usize) -> Result<Vec<u8>, Box<dyn Error>> {
    match f.decode_chunk(col_id, chunk)? {
        ColData::U8(v) => Ok(v),
        _ => Err("column type mismatch".into()),
    }
}

fn chunk_start(f: &MktFile, col: u8, chunk: usize) -> u64 {
    f.col(col).map(|c| c.chunks[chunk].start_row).unwrap_or(0)
}

// ---- Phase 3: query + resample (multi-file) ---------------------------------
// ---- Phase 3: query + resample (multi-file) ---------------------------------

/// Aggregate scan evidence across files (Drill-5 proof: files/chunks skipped,
/// rows decoded vs emitted).
#[derive(Default, Clone, Copy)]
struct AggStats {
    files_scanned: usize,
    files_skipped: usize,
    chunks_scanned: usize,
    chunks_skipped: usize,
    rows_decoded: usize,
    rows_emitted: usize,
}

/// Open every --mkt file, infer the symbol from the canonical layout
/// `data/mkt/<symbol>/dt=<period>/trades.mkt`, and group per symbol:
/// files inside one symbol are ordered by ts_min and must be time-disjoint
/// (sort-on-write contract, ADR-0002); different symbols overlap in time by
/// definition and are streamed one after another as independent streams.
fn open_grouped(mkt: &[PathBuf]) -> Result<Vec<(MktFile, PathBuf, String)>, Box<dyn Error>> {
    let mut files: Vec<(MktFile, PathBuf, String)> = mkt
        .iter()
        .map(|p| {
            let sym = p
                .parent()
                .and_then(|d| d.parent())
                .and_then(|d| d.file_name())
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_else(|| "default".to_string());
            Ok((MktFile::open(p)?, p.clone(), sym))
        })
        .collect::<Result<_, Box<dyn Error>>>()?;
    files.sort_by(|a, b| a.2.cmp(&b.2).then(a.0.meta.ts_min.cmp(&b.0.meta.ts_min)));
    let groups = group_bounds(&files);
    for (s, e) in groups.windows(2).map(|w| (w[0], w[1])) {
        for w in files[s..e].windows(2) {
            if w[0].0.meta.ts_max >= w[1].0.meta.ts_min {
                return Err(format!(
                    "files overlap in time (same symbol): {} [{}..{}] vs {} [{}..{}] -- require disjoint (symbol, day|month) partitions",
                    w[0].1.display(), w[0].0.meta.ts_min, w[0].0.meta.ts_max,
                    w[1].1.display(), w[1].0.meta.ts_min, w[1].0.meta.ts_max,
                ).into());
            }
        }
    }
    Ok(files)
}

/// Indices where the symbol changes (plus the end index) â€” group boundaries.
fn group_bounds(files: &[(MktFile, PathBuf, String)]) -> Vec<usize> {
    let mut bounds = vec![0usize];
    for i in 1..files.len() {
        if files[i].2 != files[i - 1].2 {
            bounds.push(i);
        }
    }
    bounds.push(files.len());
    bounds
}

fn query(
    mkt: Vec<PathBuf>,
    from: i64,
    to: i64,
    resample_secs: Option<u64>,
    out: Option<PathBuf>,
) -> Result<(), Box<dyn Error>> {
    if mkt.is_empty() {
        return Err("query: no --mkt given".into());
    }
    let t0 = std::time::Instant::now();
    let files = open_grouped(&mkt)?;
    let groups = group_bounds(&files);

    // Streaming path (Drill 5 RAM contract): never materialize a full Scanned
    // across all files (~5.9GB for the Sprint-2 set, 146.85M rows). Chunks
    // stream through one at a time (~40MB); we keep only aggregation output:
    //   - resample: one symbol-tagged bar stream per symbol group; the open
    //     bar is carried across chunk boundaries AND successive files of the
    //     same symbol, and reset between symbols.
    //   - count mode: rows summed from per-chunk ScanStats, no vectors kept.
    let mut stats = AggStats::default();
    let mut sink = BarSink::new(out.as_deref().filter(|_| resample_secs.is_some()))?;
    let mut cur: Option<tf_engine::Bar> = None;
    let bucket_us = resample_secs.unwrap_or(1).max(1) as i64 * 1_000_000;
    let flush_cur = |cur: &mut Option<tf_engine::Bar>,
                     sink: &mut BarSink,
                     sym: &str|
        -> Result<(), Box<dyn Error>> {
        if let Some(b) = cur.take() {
            sink.push(sym, b)?;
        }
        Ok(())
    };

    for gi in 0..(groups.len() - 1) {
        let sym = files[groups[gi]].2.clone();
        for (f, _path, _s) in &files[groups[gi]..groups[gi + 1]] {
            if to < f.meta.ts_min || from > f.meta.ts_max {
                stats.files_skipped += 1;
                stats.chunks_skipped += f.n_chunks(COL_TS)?;
                continue;
            }
            let st = tf_engine::for_each_window_chunk(f, from, to, |part| {
                let sink = &mut sink;
                match resample_secs {
                    None => Ok(()), // count-only: rows come back via st.rows_emitted
                    Some(_) => {
                        for i in 0..part.ts.len() {
                            let bucket = part.ts[i] - part.ts[i].rem_euclid(bucket_us);
                            if cur.as_ref().is_some_and(|b| b.bucket_start_us == bucket) {
                                let b = cur.as_mut().unwrap();
                                b.high = b.high.max(part.price[i]);
                                b.low = b.low.min(part.price[i]);
                                b.close = part.price[i];
                                b.base_vol += part.qty[i];
                                b.quote_vol += part.quote_qty[i];
                                b.trades += 1;
                                if part.flags[i] & tf_format::FLAG_IS_BUYER_MAKER != 0 {
                                    b.sell_base += part.qty[i];
                                } else {
                                    b.buy_base += part.qty[i];
                                }
                            } else {
                                flush_cur(&mut cur, &mut *sink, &sym)
                                    .map_err(|e| tf_format::error::MktError::Other(e.to_string()))?;
                                let is_buyer_maker =
                                    part.flags[i] & tf_format::FLAG_IS_BUYER_MAKER != 0;
                                cur = Some(tf_engine::Bar {
                                    bucket_start_us: bucket,
                                    open: part.price[i],
                                    high: part.price[i],
                                    low: part.price[i],
                                    close: part.price[i],
                                    base_vol: part.qty[i],
                                    quote_vol: part.quote_qty[i],
                                    trades: 1,
                                    buy_base: if is_buyer_maker { 0.0 } else { part.qty[i] },
                                    sell_base: if is_buyer_maker { part.qty[i] } else { 0.0 },
                                });
                            }
                        }
                        Ok(())
                    }
                }
            })?;
            stats.chunks_scanned += st.chunks_scanned;
            stats.chunks_skipped += st.chunks_skipped;
            stats.rows_decoded += st.rows_decoded;
            stats.rows_emitted += st.rows_emitted;
            stats.files_scanned += 1;
        }
        // symbol boundary: the open bar must not leak into the next stream
        flush_cur(&mut cur, &mut sink, &sym)?;
    }
    let bars_total = sink.total;
    let (min_bucket, max_bucket) = (sink.min_bucket, sink.max_bucket);
    let bars_written = sink.close()?;

    if stats.rows_emitted == 0 {
        println!(
            "files={} files_scanned={} files_skipped={} chunks_scanned={} chunks_skipped={} rows_emitted=0 elapsed_ms={}",
            files.len(), stats.files_scanned, stats.files_skipped,
            stats.chunks_scanned, stats.chunks_skipped, t0.elapsed().as_millis()
        );
        return Ok(());
    }

    match resample_secs {
        None => {
            println!(
                "files={} files_scanned={} files_skipped={} chunks_scanned={} chunks_skipped={} rows_emitted={} rows_decoded={} elapsed_ms={}",
                files.len(), stats.files_scanned, stats.files_skipped,
                stats.chunks_scanned, stats.chunks_skipped,
                stats.rows_emitted, stats.rows_decoded, t0.elapsed().as_millis()
            );
        }
        Some(secs) => {
            let wrote = bars_written > 0 && out.is_some();
            println!(
                "files={} files_scanned={} files_skipped={} chunks_scanned={} chunks_skipped={} bars={} bucket_sec={} rows_emitted={} elapsed_ms={} bars_range=[{},{}] bars_written={}",
                files.len(), stats.files_scanned, stats.files_skipped,
                stats.chunks_scanned, stats.chunks_skipped, bars_total, secs,
                stats.rows_emitted, t0.elapsed().as_millis(), min_bucket, max_bucket,
                if wrote { "parquet" } else { "stdout-only" }
            );
        }
    }
    Ok(())
}

fn write_bars_parquet(path: &Path, bars: &[tf_engine::Bar]) -> Result<bool, Box<dyn Error>> {
    let tagged: Vec<(String, tf_engine::Bar)> =
        bars.iter().map(|b| ("default".to_string(), b.clone())).collect();
    write_bars_sym_parquet(path, &tagged)
}

/// Incremental bar sink (Drill 5 RAM contract): acculates at most
/// `FLUSH_EVERY` bars in memory, then writes them as one parquet row group.
/// 18.45M bars streamed through a ≤1M-row buffer instead of a 2GB+ Vec.
pub struct BarSink {
    writer: Option<parquet::arrow::ArrowWriter<File>>,
    path: PathBuf,
    buf: Vec<(String, tf_engine::Bar)>,
    pub total: usize,
    pub min_bucket: i64,
    pub max_bucket: i64,
}

const FLUSH_EVERY: usize = 250_000; // ~25MB batch worst case; keeps RAM bounded

impl BarSink {
    pub fn new(path: Option<&Path>) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            writer: None,
            path: path.map(|p| p.to_path_buf()).unwrap_or(PathBuf::from("")),
            buf: Vec::new(),
            total: 0,
            min_bucket: i64::MAX,
            max_bucket: i64::MIN,
        })
    }

    pub fn push(&mut self, sym: &str, b: tf_engine::Bar) -> Result<(), Box<dyn Error>> {
        self.total += 1;
        self.min_bucket = self.min_bucket.min(b.bucket_start_us);
        self.max_bucket = self.max_bucket.max(b.bucket_start_us);
        self.buf.push((sym.to_string(), b));
        // No --out: bars are never needed downstream (stdout reports counters
        // only), so keep the raw Vec for total/bounds and skip flushing — but
        // resample-without-out on huge inputs is a caller-error by design.
        if self.buffered() >= FLUSH_EVERY && !self.path.as_os_str().is_empty() {
            self.flush()?;
        }
        Ok(())
    }

    fn buffered(&self) -> usize {
        self.buf.len()
    }

    fn batch(&self) -> Result<std::sync::Arc<arrow_array::RecordBatch>, Box<dyn Error>> {
        use arrow_array::{Int64Array, RecordBatch, StringArray};
        let schema = bars_schema();
        Ok(std::sync::Arc::new(RecordBatch::try_new(
            schema,
            vec![
                std::sync::Arc::new(StringArray::from_iter_values(self.buf.iter().map(|(s, _)| s.as_str()))),
                std::sync::Arc::new(Int64Array::from_iter_values(self.buf.iter().map(|(_, b)| b.bucket_start_us))),
                std::sync::Arc::new(arrow_array::Float64Array::from_iter_values(self.buf.iter().map(|(_, b)| b.open))),
                std::sync::Arc::new(arrow_array::Float64Array::from_iter_values(self.buf.iter().map(|(_, b)| b.high))),
                std::sync::Arc::new(arrow_array::Float64Array::from_iter_values(self.buf.iter().map(|(_, b)| b.low))),
                std::sync::Arc::new(arrow_array::Float64Array::from_iter_values(self.buf.iter().map(|(_, b)| b.close))),
                std::sync::Arc::new(arrow_array::Float64Array::from_iter_values(self.buf.iter().map(|(_, b)| b.base_vol))),
                std::sync::Arc::new(arrow_array::Float64Array::from_iter_values(self.buf.iter().map(|(_, b)| b.quote_vol))),
                std::sync::Arc::new(Int64Array::from_iter_values(self.buf.iter().map(|(_, b)| b.trades as i64))),
                std::sync::Arc::new(arrow_array::Float64Array::from_iter_values(self.buf.iter().map(|(_, b)| b.buy_base))),
                std::sync::Arc::new(arrow_array::Float64Array::from_iter_values(self.buf.iter().map(|(_, b)| b.sell_base))),
            ],
        )?))
    }

    fn flush(&mut self) -> Result<(), Box<dyn Error>> {
        if self.buf.is_empty() || self.path.as_os_str().is_empty() {
            return Ok(());
        }
        let batch = self.batch()?;
        let file = match &mut self.writer {
            Some(w) => {
                w.write(batch.as_ref())?;
                self.buf = Vec::with_capacity(FLUSH_EVERY);
                return Ok(());
            }
            None => File::create(&self.path)?,
        };
        let mut w = bars_writer(file);
        w.write(batch.as_ref())?;
        self.writer = Some(w);
        self.buf = Vec::with_capacity(FLUSH_EVERY);
        Ok(())
    }

    pub fn close(mut self) -> Result<usize, Box<dyn Error>> {
        self.flush()?;
        if let Some(w) = self.writer.take() {
            w.close()?;
        }
        Ok(self.total)
    }
}

fn bars_schema() -> std::sync::Arc<arrow_schema::Schema> {
    std::sync::Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("symbol", arrow_schema::DataType::Utf8, false),
        arrow_schema::Field::new("ts_bucket", arrow_schema::DataType::Int64, false),
        arrow_schema::Field::new("open", arrow_schema::DataType::Float64, false),
        arrow_schema::Field::new("high", arrow_schema::DataType::Float64, false),
        arrow_schema::Field::new("low", arrow_schema::DataType::Float64, false),
        arrow_schema::Field::new("close", arrow_schema::DataType::Float64, false),
        arrow_schema::Field::new("base_vol", arrow_schema::DataType::Float64, false),
        arrow_schema::Field::new("quote_vol", arrow_schema::DataType::Float64, false),
        arrow_schema::Field::new("trades", arrow_schema::DataType::Int64, false),
        arrow_schema::Field::new("buy_base", arrow_schema::DataType::Float64, false),
        arrow_schema::Field::new("sell_base", arrow_schema::DataType::Float64, false),
    ]))
}

/// Small-budget writer props (Drill 5 RAM contract): small row groups and data
/// pages so the parquet encoder itself cannot balloon the working set.
fn bars_writer(file: File) -> parquet::arrow::ArrowWriter<File> {
    use parquet::file::properties::WriterProperties;
    use parquet::basic::Compression;
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(250_000))
        .set_data_page_size_limit(512 * 1024)
        .set_write_batch_size(64 * 1024)
        .set_compression(Compression::ZSTD(Default::default()))
        .build();
    parquet::arrow::ArrowWriter::try_new(file, bars_schema(), Some(props)).unwrap()
}

/// Legacy one-shot writer (used by roundtrip-sized outputs and tests).
fn write_bars_sym_parquet(
    path: &Path,
    bars: &[(String, tf_engine::Bar)],
) -> Result<bool, Box<dyn Error>> {
    let mut sink = BarSink::new(Some(path))?;
    for (s, b) in bars {
        sink.push(s, b.clone())?;
    }
    sink.close()?;
    Ok(true)
}

// ---- Phase 3: replay (Drill 6) ----------------------------------------------

fn replay(
    mkt: PathBuf,
    from: i64,
    to: i64,
    speed: Option<f64>,
    resample_secs: Option<u64>,
    out: Option<PathBuf>,
) -> Result<(), Box<dyn Error>> {
    use std::time::{Duration, Instant};

    let sp = speed.filter(|s| *s > 0.0);
    let f = MktFile::open(&mkt)?;
    let (sc, _st) = tf_engine::scan_ts_range(&f, from, to)?;

    let bucket_us = resample_secs.unwrap_or(1).max(1) as i64 * 1_000_000;
    let mut bars: Vec<tf_engine::Bar> = Vec::new();
    let mut prev_ts: Option<i64> = None;
    let mut slept = Duration::ZERO;
    let mut gaps_skipped: u64 = 0; // silent stretches > 10s get skipped for pacing
    let started = Instant::now();

    for i in 0..sc.row_count() {
        let ts = sc.ts[i];
        if let Some(speed) = sp {
            if let Some(prev) = prev_ts {
                let gap_us = ts - prev;
                if gap_us <= MAX_PACED_GAP_SEC * 1_000_000 {
                    slept += sleep_for(gap_us, speed);
                } else {
                    // a silent stretch would stall the demo; note and jump
                    gaps_skipped += 1;
                }
            }
        }
        prev_ts = Some(ts);

        // incremental bar aggregation (must mirror tf_engine::resample exactly)
        let bucket = ts - ts.rem_euclid(bucket_us);
        let (buy, sell) = if sc.flags[i] & tf_format::FLAG_IS_BUYER_MAKER != 0 {
            (0.0, sc.qty[i])
        } else {
            (sc.qty[i], 0.0)
        };
        if bars.last_mut().map_or(false, |b| b.bucket_start_us == bucket) {
            let b = bars.last_mut().unwrap();
            b.high = b.high.max(sc.price[i]);
            b.low = b.low.min(sc.price[i]);
            b.close = sc.price[i];
            b.base_vol += sc.qty[i];
            b.quote_vol += sc.quote_qty[i];
            b.trades += 1;
            b.buy_base += buy;
            b.sell_base += sell;
        } else {
            bars.push(tf_engine::Bar {
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
            });
        }
    }

    let event_span_us = prev_ts.unwrap_or(from) - sc.ts.first().copied().unwrap_or(from);
    let expected_paced_us = event_span_us as f64 / sp.unwrap_or(f64::INFINITY);
    let wrote = if let (Some(p), Some(_)) = (&out, resample_secs) {
        // tag bars with the symbol from the canonical layout, same as `query`
        let sym = mkt
            .parent()
            .and_then(|d| d.parent())
            .and_then(|d| d.file_name())
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "default".to_string());
        let tagged: Vec<(String, tf_engine::Bar)> =
            bars.iter().map(|b| (sym.clone(), b.clone())).collect();
        write_bars_sym_parquet(p, &tagged)?
    } else {
        false
    };

    println!(
        "events={} bars={} speed={} wall_ms={} slept_ms={} event_ms={} pacing_x={:.2} gaps_skipped={} bars_written={}",
        sc.row_count(),
        bars.len(),
        sp.unwrap_or(0.0),
        started.elapsed().as_millis(),
        slept.as_millis(),
        expected_paced_us / 1000.0,
        if expected_paced_us.is_infinite() { 0.0 } else {
            slept.as_micros() as f64 / expected_paced_us
        },
        gaps_skipped,
        if wrote { "parquet" } else { "none" },
    );
    Ok(())
}

/// Cap on paced gaps so old data replays don't stall the demo for minutes.
/// Replays above this stretch are jumped (counted in `gaps_skipped`).
const MAX_PACED_GAP_SEC: i64 = 10;

/// Sleep the pacing delay for one event gap at `speed`, clamped to sane bounds.
/// Per-event gaps are microseconds at spot granularity, so caller-side debt
/// accumulation isn't needed for max-rate runs (speed=0 sleeps nothing).
fn sleep_for(gap_us: i64, speed: f64) -> std::time::Duration {
    let d = std::time::Duration::from_micros(((gap_us as f64 / speed) as u64).min(MAX_SLEEP_MS * 1000));
    if !d.is_zero() {
        std::thread::sleep(d);
    }
    d
}
const MAX_SLEEP_MS: u64 = 2_500;

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::IngestNorm { input, out, chunk_rows } => ingest_norm(input, out, chunk_rows),
        Cmd::Roundtrip { mkt, input } => roundtrip(mkt, input),
        Cmd::Query { mkt, from, to, resample, out } => query(mkt, from, to, resample, out),
        Cmd::Replay { mkt, from, to, speed, resample, out } => {
            replay(mkt, from, to, speed, resample, out)
        }
    }
}
