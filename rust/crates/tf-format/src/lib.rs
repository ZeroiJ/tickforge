//! `.mkt` custom columnar format — implements `docs/format-spec.md` v1.
//!
//! Layout: header (magic+version) → column sections (6 physical columns, chunked,
//! zstd payloads) → footer body (metadata + per-column chunk index) → footer_len
//! (u32 LE) → magic `"TFMK"`. Readers parse last-8-bytes magic first, so a file
//! killed before its atomic rename is rejected before any column data is touched.

pub const MAGIC: [u8; 4] = *b"TFMK";
pub const FORMAT_VERSION: u16 = 1;

/// Per spec §3: chunks of ~10^6 rows, aligned across all columns.
pub const CHUNK_ROWS_MAX: u32 = 1_000_000;

/// Default writer chunk size (tests use a small override to exercise
/// multi-chunk paths without allocating 10^6 rows).
pub const CHUNK_ROWS_DEFAULT: u32 = CHUNK_ROWS_MAX;

/// zstd compression level (0.13 default is 3; pinned for reproducible sizes —
/// and deterministic output means re-ingest of identical input is byte-identical,
/// which is what makes Drill 1 idempotency observable at the byte level).
pub const ZSTD_LEVEL: i32 = 3;

// Physical column ids (docs/format-spec.md §2). `symbol` lives in the footer,
// the two bools are bit-packed into the u8 flags column.
pub const COL_TRADE_ID: u8 = 0;
pub const COL_PRICE: u8 = 1;
pub const COL_QTY: u8 = 2;
pub const COL_QUOTE_QTY: u8 = 3;
pub const COL_TS: u8 = 4;
pub const COL_FLAGS: u8 = 5;

/// bit i of the packed flags column (schema.rs `is_buyer_maker` / `is_best_match`)
pub const FLAG_IS_BUYER_MAKER: u8 = 1 << 0;
pub const FLAG_IS_BEST_MATCH: u8 = 1 << 1;

pub mod error;
pub mod reader;
pub mod writer;

// Re-exports: the ergonomic public API surface.
pub use reader::{col_kind, FileMeta, ColIdx, ColKind, MktFile};
pub use writer::{write_mkt, write_mkt_chunked, ColumnIn, WriteStats};

/// Payload codec byte (per column, footer index).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Codec {
    Plain = 0,
    Zstd = 1,
}

impl TryFrom<u8> for Codec {
    type Error = error::MktError;
    fn try_from(b: u8) -> Result<Self, Self::Error> {
        match b {
            0 => Ok(Codec::Plain),
            1 => Ok(Codec::Zstd),
            other => Err(error::MktError::CodecUnknown(other)),
        }
    }
}

/// One chunk's index entry — fixed 52-byte little-endian layout shared by the
/// writer and reader (spec §4). `min/max` are raw i64 per the spec §4 bit-pattern
/// convention; only the `ts` column's bounds drive v1 range pruning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkIdx {
    pub start_row: u64,
    pub row_count: u32,
    pub min: i64,
    pub max: i64,
    pub offset: u64,
    pub comp_len: u64,
    pub uncomp_len: u64,
}

/// Decoded column values. Widths: I64/F64 = 8 B little-endian, U8 = 1 B.
#[derive(Debug, Clone, PartialEq)]
pub enum ColData {
    I64(Vec<i64>),
    F64(Vec<f64>),
    U8(Vec<u8>),
}

impl ColData {
    pub fn len(&self) -> usize {
        match self {
            ColData::I64(v) => v.len(),
            ColData::F64(v) => v.len(),
            ColData::U8(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Wire format: contiguous native-width little-endian values (spec §2).
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            ColData::I64(v) => v.iter().flat_map(|x| x.to_le_bytes()).collect(),
            ColData::F64(v) => v.iter().flat_map(|x| x.to_le_bytes()).collect(),
            ColData::U8(v) => v.clone(),
        }
    }

    /// Chunk min/max as raw i64 per spec §4: numerically for integers; for f64
    /// payloads the IEEE bit pattern reinterpreted as i64 (documented v1
    /// simplification — valid total order for the non-negative ticks we store).
    pub fn minmax_raw(&self, range: (usize, usize)) -> (i64, i64) {
        let (start, end) = range;
        match self {
            ColData::I64(v) => {
                let mut lo = i64::MAX;
                let mut hi = i64::MIN;
                for &x in &v[start..end] {
                    lo = lo.min(x);
                    hi = hi.max(x);
                }
                (lo, hi)
            }
            ColData::F64(v) => {
                let mut lo = i64::MAX;
                let mut hi = i64::MIN;
                for &f in &v[start..end] {
                    let b = f.to_bits() as i64;
                    lo = lo.min(b);
                    hi = hi.max(b);
                }
                (lo, hi)
            }
            ColData::U8(v) => {
                let mut lo = i64::MAX;
                let mut hi = i64::MIN;
                for &b in &v[start..end] {
                    let x = i64::from(b);
                    lo = lo.min(x);
                    hi = hi.max(x);
                }
                (lo, hi)
            }
        }
    }
}
