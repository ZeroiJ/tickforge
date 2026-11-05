//! Writer (spec §6): append-once, time-ordered, atomic rename.
//!
//! Kill-safety contract: everything goes to `<path>.tmp`, then
//! flush → fsync → rename. A process killed at *any* point leaves either the
//! previous `.mkt` or a `.tmp` — never a half-valid file at the final path.

use std::fs::File;
use std::io::Write;
use std::path::Path;

use crate::error::MktError;
use crate::{ChunkIdx, Codec, ColData, CHUNK_ROWS_DEFAULT, FORMAT_VERSION, MAGIC, ZSTD_LEVEL};

/// One physical column of the output file (spec §2), values in row order.
#[derive(Clone)]
pub struct ColumnIn {
    pub id: u8,
    pub codec: Codec,
    pub data: ColData,
}

#[derive(Debug, Clone)]
pub struct WriteStats {
    pub rows: u64,
    pub chunks: u32,
    pub ts_min: i64,
    pub ts_max: i64,
    /// final file size in bytes (data + footer, incl. header + footer tail)
    pub file_bytes: u64,
    /// total uncompressed column bytes (for the benchmark table)
    pub uncomp_bytes: u64,
}

pub(crate) fn put_u8(buf: &mut Vec<u8>, v: u8) {
    buf.push(v);
}
pub(crate) fn put_u16(buf: &mut Vec<u8>, v: u16) {
    buf.extend_from_slice(&v.to_le_bytes());
}
pub(crate) fn put_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}
pub(crate) fn put_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

/// Writes `columns` as one `.mkt` file at `path` (atomic rename). See spec §6.
///
/// Requirements checked loudly: ids are exactly {0..=5} each once, all columns
/// have equal row counts > 0, col 4 (`ts`) present and I64.
pub fn write_mkt(
    path: &Path,
    symbol: &str,
    columns: Vec<ColumnIn>,
) -> Result<WriteStats, MktError> {
    write_mkt_chunked(path, symbol, columns, CHUNK_ROWS_DEFAULT)
}

/// Same as [`write_mkt`] with an explicit chunk size (tests use this to hit
/// multi-chunk paths without allocating 10^6-row fixtures).
pub fn write_mkt_chunked(
    path: &Path,
    symbol: &str,
    mut columns: Vec<ColumnIn>,
    chunk_rows_max: u32,
) -> Result<WriteStats, MktError> {
    // ---- validate input ----------------------------------------------------
    if symbol.is_empty() {
        return Err(MktError::InvalidInput("empty symbol".into()));
    }
    if symbol.len() > u16::MAX as usize {
        return Err(MktError::InvalidInput("symbol too long".into()));
    }

    let mut seen = [false; 6];
    for col in &columns {
        let id = col.id as usize;
        if id >= 6 {
            return Err(MktError::UnknownColumn(col.id));
        }
        if seen[id] {
            return Err(MktError::InvalidInput(format!("duplicate column id {id}")));
        }
        seen[id] = true;
    }
    let hard_ids: [u8; 6] = (0..=5).collect::<Vec<u8>>().try_into().unwrap();
    for id in hard_ids {
        if !seen[id as usize] {
            return Err(MktError::InvalidInput(format!("missing column id {id}")));
        }
    }

    columns.sort_by_key(|c| c.id); // file layout: sections in ascending id order

    let rows = columns[0].data.len() as u64;
    if rows == 0 {
        return Err(MktError::InvalidInput("zero-row input".into()));
    }
    for col in &columns {
        if col.data.len() as u64 != rows {
            return Err(MktError::InvalidInput(format!(
                "column {} has {} rows, expected {rows}",
                col.id,
                col.data.len()
            )));
        }
    }

    let ts_idx = columns.iter().position(|c| c.id == crate::COL_TS).unwrap();
    if !matches!(columns[ts_idx].data, ColData::I64(_)) {
        return Err(MktError::TypeMismatch { id: 4, expected: "i64" });
    }

    // ---- chunk plan (aligned across all columns, spec §3) -------------------
    if chunk_rows_max == 0 {
        return Err(MktError::InvalidInput("chunk_rows_max == 0".into()));
    }
    let chunk_rows = u64::from(chunk_rows_max);
    let n_chunks = rows.div_ceil(chunk_rows);

    let tmp_path = path.with_extension("mkt.tmp");
    let mut f = File::create(&tmp_path)?;

    // header (spec §1): magic + version + 2 reserved bytes (zero)
    f.write_all(&MAGIC)?;
    f.write_all(&FORMAT_VERSION.to_le_bytes())?;
    f.write_all(&[0u8; 2])?;

    let mut index_buf: Vec<u8> = Vec::new();
    let mut data_bytes: u64 = 8; // header
    let mut uncomp_bytes: u64 = 0;
    let mut raw_buf: Vec<u8> = Vec::new();
    let mut payload_buf: Vec<u8> = Vec::new();

    for col in &columns {
        let mut chunks = Vec::with_capacity(n_chunks as usize);
        for k in 0..n_chunks {
            let start = k * chunk_rows;
            let end = ((k + 1) * chunk_rows).min(rows);
            let row_count = (end - start) as u32;

            let (min, max) = col.data.minmax_raw((start as usize, end as usize));

            // chunk payload: contiguous LE values (spec §2), then codec per column
            raw_buf.clear();
            match &col.data {
                ColData::I64(v) => {
                    for x in &v[start as usize..end as usize] {
                        raw_buf.extend_from_slice(&x.to_le_bytes());
                    }
                }
                ColData::F64(v) => {
                    for x in &v[start as usize..end as usize] {
                        raw_buf.extend_from_slice(&x.to_le_bytes());
                    }
                }
                ColData::U8(v) => raw_buf.extend_from_slice(&v[start as usize..end as usize]),
            }
            let uncomp_len = raw_buf.len() as u64;

            payload_buf.clear();
            match col.codec {
                Codec::Plain => payload_buf.extend_from_slice(&raw_buf),
                Codec::Zstd => {
                    payload_buf = zstd::stream::encode_all(&raw_buf[..], ZSTD_LEVEL)
                        .map_err(|e| MktError::Zstd(e.to_string()))?;
                }
            }
            let comp_len = payload_buf.len() as u64;

            f.write_all(&payload_buf)?;
            chunks.push(ChunkIdx {
                start_row: start,
                row_count,
                min,
                max,
                offset: data_bytes,
                comp_len,
                uncomp_len,
            });
            data_bytes += comp_len;
            uncomp_bytes += uncomp_len;
        }

        // column index entry (spec §4): id, codec, n_chunks, fixed-52-byte entries
        put_u8(&mut index_buf, col.id);
        put_u8(&mut index_buf, col.codec as u8);
        put_u32(&mut index_buf, n_chunks as u32);
        for c in &chunks {
            put_u64(&mut index_buf, c.start_row);
            put_u32(&mut index_buf, c.row_count);
            put_u64(&mut index_buf, c.min as u64);
            put_u64(&mut index_buf, c.max as u64);
            put_u64(&mut index_buf, c.offset);
            put_u64(&mut index_buf, c.comp_len);
            put_u64(&mut index_buf, c.uncomp_len);
        }
    }

    // ---- footer body (spec §4) ---------------------------------------------
    let (ts_min, ts_max) = columns[ts_idx].data.minmax_raw((0, rows as usize));

    let mut footer: Vec<u8> = Vec::new();
    footer.extend_from_slice(&MAGIC);
    put_u16(&mut footer, FORMAT_VERSION);
    put_u32(&mut footer, tf_core::schema::SCHEMA_VERSION);
    put_u64(&mut footer, rows);
    put_u32(&mut footer, chunk_rows_max);
    put_u64(&mut footer, ts_min as u64);
    put_u64(&mut footer, ts_max as u64);
    put_u16(&mut footer, symbol.len() as u16);
    footer.extend_from_slice(symbol.as_bytes());
    footer.extend_from_slice(&index_buf);

    f.write_all(&footer)?;
    // footer_len + trailing magic = last 8 bytes of the file (spec §0)
    f.write_all(&(footer.len() as u32).to_le_bytes())?;
    f.write_all(&MAGIC)?;

    // flush + fsync + atomic rename (kill-safe, spec §6)
    f.sync_data()?;
    drop(f);
    std::fs::rename(&tmp_path, path)?;

    Ok(WriteStats {
        rows,
        chunks: n_chunks as u32,
        ts_min,
        ts_max,
        file_bytes: data_bytes + footer.len() as u64 + 8,
        uncomp_bytes,
    })
}
