//! Reader (spec §5): memmap the file, parse the footer FIRST, then serve typed
//! chunk reads straight off the map. Never `read_to_end` — dev rule AGENTS.md §4.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::Cursor;
use std::path::Path;

use memmap2::Mmap;

use crate::error::MktError;
use crate::reader::ColKind::{F64, I64, U8};
use crate::{ChunkIdx, Codec, ColData, COL_QUOTE_QTY, COL_PRICE, COL_QTY, COL_FLAGS, COL_TRADE_ID, COL_TS, FORMAT_VERSION, MAGIC};

/// Logical type of a physical column (spec §2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColKind {
    I64,
    F64,
    U8,
}

/// Column id → logical type. Unknown ids fail loudly (UnknownColumn).
pub fn col_kind(id: u8) -> Result<ColKind, MktError> {
    match id {
        COL_TRADE_ID | COL_TS => Ok(I64),
        COL_PRICE | COL_QTY | COL_QUOTE_QTY => Ok(F64),
        COL_FLAGS => Ok(U8),
        other => Err(MktError::UnknownColumn(other)),
    }
}

/// One parsed column: id, codec, type, and its chunk index.
#[derive(Debug, Clone)]
pub struct ColIdx {
    pub id: u8,
    pub codec: Codec,
    pub kind: ColKind,
    pub chunks: Vec<ChunkIdx>,
}

/// Footer metadata block (spec §4).
#[derive(Debug, Clone)]
pub struct FileMeta {
    pub symbol: String,
    pub row_count: u64,
    pub chunk_rows_max: u32,
    pub ts_min: i64,
    pub ts_max: i64,
    pub schema_version: u32,
}

/// An open `.mkt` file: memmap + fully-validated footer index.
///
/// `_file` is kept so the mmap stays valid for the struct's lifetime.
#[derive(Debug)]
pub struct MktFile {
    _file: File,
    map: Mmap,
    pub meta: FileMeta,
    cols: BTreeMap<u8, ColIdx>,
    /// absolute byte offset where the footer body begins (= end of payload data)
    data_end: u64,
    /// total mapped length
    len: u64,
}

// ---- little-endian cursor over the footer body ------------------------------

struct Cur<'a> {
    b: &'a [u8],
    pos: usize,
    end: usize,
}

impl<'a> Cur<'a> {
    fn new(b: &'a [u8]) -> Self {
        let end = b.len();
        Self { b, pos: 0, end }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], MktError> {
        if self.end - self.pos < n {
            return Err(MktError::FooterTruncated {
                needed: n - (self.end - self.pos),
                available: self.end - self.pos,
            });
        }
        let s = &self.b[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    fn u8(&mut self) -> Result<u8, MktError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, MktError> {
        let s = self.take(2)?;
        Ok(u16::from_le_bytes([s[0], s[1]]))
    }
    fn u32(&mut self) -> Result<u32, MktError> {
        let s = self.take(4)?;
        Ok(u32::from_le_bytes(s.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, MktError> {
        let s = self.take(8)?;
        Ok(u64::from_le_bytes(s.try_into().unwrap()))
    }
    fn i64(&mut self) -> Result<i64, MktError> {
        Ok(self.u64()? as i64)
    }
}

impl MktFile {
    /// Opens a `.mkt` file and validates header + footer + index arithmetic
    /// before any column payload is decoded (spec §5).
    pub fn open(path: &Path) -> Result<MktFile, MktError> {
        let file = File::open(path)?;
        let len = file.metadata()?.len();
        let map = unsafe { Mmap::map(&file)? };
        let m = &map[..];

        // header check (spec §1): a valid file always starts with magic+version+zeros
        if len < 8 {
            return Err(MktError::BadMagic(format!(
                "file is {len} bytes, smaller than the 8-byte header"
            )));
        }
        if m[0..4] != MAGIC {
            return Err(MktError::BadMagic("header magic".into()));
        }
        let header_version = u16::from_le_bytes(m[4..6].try_into().unwrap());
        if header_version != FORMAT_VERSION {
            return Err(MktError::VersionMismatch {
                got: header_version,
                expected: FORMAT_VERSION,
            });
        }
        if m[6..8] != [0u8, 0u8] {
            return Err(MktError::BadMagic("header reserved bytes are not zero".into()));
        }

        // last 8 bytes: footer_len (u32 LE) + trailing magic
        let tail = &m[(len - 8) as usize..];
        if tail[4..8] != MAGIC {
            return Err(MktError::BadMagic("trailing magic (last 4 bytes)".into()));
        }
        let footer_len = u32::from_le_bytes(tail[..4].try_into().unwrap()) as usize;

        // footer body slice
        let body_start = (len as usize)
            .checked_sub(8 + footer_len)
            .ok_or(MktError::FooterTruncated {
                needed: 8 + footer_len,
                available: len as usize,
            })?;
        let mut cur = Cur::new(&m[body_start..len as usize - 8]);

        // meta block (spec §4)
        if cur.take(4)? != MAGIC {
            return Err(MktError::BadMagic("footer body magic".into()));
        }
        let version = cur.u16()?;
        if version != FORMAT_VERSION {
            return Err(MktError::VersionMismatch { got: version, expected: FORMAT_VERSION });
        }
        let schema_version = cur.u32()?;
        let row_count = cur.u64()?;
        let chunk_rows_max = cur.u32()?;
        let ts_min = cur.i64()?;
        let ts_max = cur.i64()?;
        if row_count == 0 {
            return Err(MktError::IndexMismatch("footer row_count == 0".into()));
        }
        if chunk_rows_max == 0 {
            return Err(MktError::IndexMismatch("footer chunk_rows_max == 0".into()));
        }
        let symbol_len = cur.u16()? as usize;
        let symbol_bytes = cur.take(symbol_len)?;
        let symbol = String::from_utf8(symbol_bytes.to_vec())
            .map_err(|_| MktError::IndexMismatch("footer symbol is not UTF-8".into()))?;

        // column index: id, codec, n_chunks, then fixed chunk entries
        let mut cols: BTreeMap<u8, ColIdx> = BTreeMap::new();
        let mut prev_id: Option<u8> = None;
        while cur.pos < cur.end {
            let id = cur.u8()?;
            if col_kind(id).is_err() {
                return Err(MktError::UnknownColumn(id));
            }
            if let Some(prev) = prev_id {
                if id <= prev {
                    return Err(MktError::IndexMismatch(format!(
                        "column ids not strictly ascending ({prev} then {id})"
                    )));
                }
            }
            prev_id = Some(id);
            let codec = Codec::try_from(cur.u8()?)?;
            let n_chunks = cur.u32()?;
            let mut chunks = Vec::with_capacity(n_chunks as usize);
            let mut running: u64 = 0;
            for k in 0..n_chunks {
                let start_row = cur.u64()?;
                let row_count = cur.u32()?;
                let min = cur.i64()?;
                let max = cur.i64()?;
                let offset = cur.u64()?;
                let comp_len = cur.u64()?;
                let uncomp_len = cur.u64()?;
                if start_row != running {
                    return Err(MktError::IndexMismatch(format!(
                        "col {id} chunk {k}: start_row {start_row} != expected {running}"
                    )));
                }
                running += u64::from(row_count);
                if row_count == 0 || row_count > chunk_rows_max {
                    return Err(MktError::IndexMismatch(format!(
                        "col {id} chunk {k}: row_count {row_count} outside 1..={chunk_rows_max}"
                    )));
                }
                if min > max {
                    return Err(MktError::IndexMismatch(format!(
                        "col {id} chunk {k}: min {min} > max {max}"
                    )));
                }
                if offset + comp_len > body_start as u64 {
                    return Err(MktError::IndexMismatch(format!(
                        "col {id} chunk {k}: payload [{offset}..{}] overruns data region {}",
                        offset + comp_len, body_start
                    )));
                }
                chunks.push(ChunkIdx {
                    start_row,
                    row_count,
                    min,
                    max,
                    offset,
                    comp_len,
                    uncomp_len,
                });
            }
            if running != row_count {
                return Err(MktError::IndexMismatch(format!(
                    "col {id}: chunk rows sum {running} != footer row_count {row_count}"
                )));
            }
            let idx = ColIdx { id, codec, kind: col_kind(id)?, chunks };
            // cheap integrity cross-check: file-level ts bounds == ts-column bounds
            if id == COL_TS {
                let first = idx.chunks.first().unwrap().min;
                let last = idx.chunks.last().unwrap().max;
                if first != ts_min || last != ts_max {
                    return Err(MktError::IndexMismatch(format!(
                        "file ts range [{ts_min},{ts_max}] != ts-chunk range [{first},{last}]"
                    )));
                }
            }
            cols.insert(id, idx);
        }
        // columns after the meta block are mandatory for v1 (spec §2: exactly 6)
        if cols.len() != 6 {
            return Err(MktError::IndexMismatch(format!(
                "expected 6 columns, footer index has {}",
                cols.len()
            )));
        }
        if !cols.contains_key(&COL_TS) {
            return Err(MktError::TsColumnMissing);
        }

        Ok(MktFile {
            _file: file,
            map,
            meta: FileMeta {
                symbol,
                row_count,
                chunk_rows_max,
                ts_min,
                ts_max,
                schema_version,
            },
            cols,
            data_end: body_start as u64,
            len,
        })
    }

    pub fn col(&self, id: u8) -> Result<&ColIdx, MktError> {
        self.cols.get(&id).ok_or(MktError::UnknownColumn(id))
    }

    /// Number of chunks in a column.
    pub fn n_chunks(&self, id: u8) -> Result<usize, MktError> {
        Ok(self.col(id)?.chunks.len())
    }

    /// Row range `[t1, t2]` (inclusive, µs) → indexes of ts chunks that could
    /// contain rows. Uses ONLY stored min/max — no payload is read here; that
    /// skip-without-reading property is what Phase 3 pruning is built on.
    pub fn chunks_for_ts_range(&self, t1: i64, t2: i64) -> Result<Vec<usize>, MktError> {
        let ts = self.col(COL_TS)?;
        Ok(ts
            .chunks
            .iter()
            .enumerate()
            .filter(|(_, c)| c.max >= t1 && c.min <= t2)
            .map(|(i, _)| i)
            .collect())
    }

    /// Payload slice for a chunk, bounds-checked against the data region.
    fn payload(&self, c: &ChunkIdx) -> Result<&[u8], MktError> {
        let start = c.offset as usize;
        let end = start + c.comp_len as usize;
        if end > self.data_end as usize {
            return Err(MktError::IndexMismatch(format!(
                "chunk payload [{start}..{end}) overruns data region {}",
                self.data_end
            )));
        }
        Ok(&self.map[start..end])
    }

    /// Decodes one chunk of `col_id` into typed values (spec §5).
    pub fn decode_chunk(&self, col_id: u8, chunk_no: usize) -> Result<ColData, MktError> {
        let col = self.col(col_id)?;
        let c = *col
            .chunks
            .get(chunk_no)
            .ok_or_else(|| MktError::IndexMismatch(format!("col {col_id}: no chunk {chunk_no}")))?;
        let payload = self.payload(&c)?;
        let raw: Vec<u8> = match col.codec {
            Codec::Plain => payload.to_vec(),
            Codec::Zstd => {
                let out = zstd::stream::decode_all(Cursor::new(payload))
                    .map_err(|e| MktError::Zstd(e.to_string()))?;
                if out.len() as u64 != c.uncomp_len {
                    return Err(MktError::IndexMismatch(format!(
                        "col {col_id} chunk {chunk_no}: decoded {} bytes, index says {}",
                        out.len(),
                        c.uncomp_len
                    )));
                }
                out
            }
        };
        if raw.len() != c.uncomp_len as usize {
            return Err(MktError::IndexMismatch(format!(
                "col {col_id} chunk {chunk_no}: payload {} bytes != uncomp_len {}",
                raw.len(),
                c.uncomp_len
            )));
        }
        match col.kind {
            I64 => Ok(ColData::I64(
                raw.chunks_exact(8)
                    .map(|b| i64::from_le_bytes(b.try_into().unwrap()))
                    .collect(),
            )),
            F64 => Ok(ColData::F64(
                raw.chunks_exact(8)
                    .map(|b| f64::from_le_bytes(b.try_into().unwrap()))
                    .collect(),
            )),
            U8 => Ok(ColData::U8(raw)),
        }
    }

    /// Decodes all chunks of a column into one value vec, in row order.
    pub fn decode_column(&self, col_id: u8) -> Result<ColData, MktError> {
        let n = self.n_chunks(col_id)?;
        let mut i64v: Vec<i64> = Vec::new();
        let mut f64v: Vec<f64> = Vec::new();
        let mut u8v: Vec<u8> = Vec::new();
        for k in 0..n {
            match self.decode_chunk(col_id, k)? {
                ColData::I64(v) => i64v.extend(v),
                ColData::F64(v) => f64v.extend(v),
                ColData::U8(v) => u8v.extend(v),
            }
        }
        let out = match col_kind(col_id)? {
            I64 => ColData::I64(i64v),
            F64 => ColData::F64(f64v),
            U8 => ColData::U8(u8v),
        };
        let expect = self.meta.row_count as usize;
        if out.len() != expect {
            return Err(MktError::IndexMismatch(format!(
                "col {col_id}: decoded {} rows, meta says {expect}",
                out.len()
            )));
        }
        Ok(out)
    }

    #[allow(dead_code)] // kept for engine-phase diagnostics
    pub fn file_len(&self) -> u64 {
        self.len
    }
}
