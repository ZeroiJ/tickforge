//! Errors — one variant per corruption class (spec §5). Fail loudly, never repair.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum MktError {
    #[error("bad magic: expected 'TFMK' ({0})")]
    BadMagic(String),

    #[error("format version {got} != supported {expected}")]
    VersionMismatch { got: u16, expected: u16 },

    #[error("footer truncated: needs {needed} more bytes than {available} available")]
    FooterTruncated { needed: usize, available: usize },

    #[error("footer index mismatch: {0}")]
    IndexMismatch(String),

    #[error("unknown codec byte {0}")]
    CodecUnknown(u8),

    #[error("column id {0} is not a defined physical column (0..=5)")]
    UnknownColumn(u8),

    #[error("column {id} type mismatch: expected {expected}")]
    TypeMismatch { id: u8, expected: &'static str },

    #[error("ts column (id 4) missing — required for range pruning")]
    TsColumnMissing,

    #[error("invalid writer input: {0}")]
    InvalidInput(String),

    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),

    #[error("zstd: {0}")]
    Zstd(String),
}
