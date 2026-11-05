//! `tf-cli` — the single tickforge binary.
//!
//! Phase 2 surface (plan.md):
//! - `ingest-norm`  : normalized canonical Parquet → `.mkt` (atomic, idempotent, sha-keyed)
//! - `roundtrip`    : decode `.mkt` back and compare to Parquet values bitwise (spec §7 proof)

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
                    return Err(format!("null in key column at row {} — contract violation", trade_id.len()).into());
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
                "file contains multiple symbols {symbols:?} — one .mkt file per (symbol, day)"
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
    let canon = Canonical::read(&input)?;
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
        "format": "mkt-v1",
        "spec": "docs/format-spec.md",
    });
    serde_json::to_writer_pretty(File::create(&sidecar)?, &side)?;

    println!(
        "rows_written={} chunks={} mkt_bytes={} uncomp_bytes={} out_path={}",
        stats.rows, stats.chunks, stats.file_bytes, stats.uncomp_bytes, mkt_path.display()
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

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::IngestNorm { input, out, chunk_rows } => ingest_norm(input, out, chunk_rows),
        Cmd::Roundtrip { mkt, input } => roundtrip(mkt, input),
    }
}
