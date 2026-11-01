"""tickforge canonical tick schema — SINGLE SOURCE OF TRUTH (Python side).

Rust mirror: rust/crates/tf-core/src/schema.rs — keep them field-for-field identical.
Any change here must be mirrored there (and vice versa) in the same commit.

Source contract: binance-public-data spot aggTrades CSV (headerless, 8 columns),
timestamps in MICROSECONDS for spot data from 2025-01-01 onwards.
"""

from __future__ import annotations

from dataclasses import dataclass

SCHEMA_VERSION = 1

# Canonical column order + types (matches Polars/Arrow dtypes)
CANONICAL_FIELDS: tuple[tuple[str, str], ...] = (
    ("trade_id", "int64"),        # aggregated trade id (unique-ish per symbol/day; csv u64)
    ("symbol", "string"),         # e.g. BTCUSDT (uppercased)
    ("price", "float64"),         # aggregated price — all merged trades share one price
    ("qty", "float64"),           # summed base quantity of the aggregate
    ("quote_qty", "float64"),     # DERIVED = price * qty (exact for aggTrades; see ADR note)
    ("ts", "int64"),              # event time in MICROSECONDS since epoch (canonical unit)
    ("is_buyer_maker", "bool"),   # True = the buy side placed the resting order (maker)
    ("is_best_match", "bool"),    # was the trade the best price match
)

CANONICAL_NAMES: tuple[str, ...] = tuple(name for name, _ in CANONICAL_FIELDS)

# Raw spot aggTrades CSV column order (headerless) — used by the normalizer.
AGGTRADES_CSV_COLUMNS: tuple[tuple[str, str], ...] = (
    ("agg_trade_id", "int64"),
    ("price", "float64"),
    ("qty", "float64"),
    ("first_trade_id", "int64"),
    ("last_trade_id", "int64"),
    ("ts", "int64"),
    ("is_buyer_maker", "bool"),
    ("is_best_match", "bool"),
)

# CSV -> canonical mapping: renames table. Columns not listed here are DROPPED.
CSV_TO_CANONICAL: dict[str, str] = {
    "agg_trade_id": "trade_id",
    "price": "price",
    "qty": "qty",
    "ts": "ts",
    "is_buyer_maker": "is_buyer_maker",
    "is_best_match": "is_best_match",
}

# first/last trade_id are aggTrades-only provenance; not needed to build bars (Phase 1).
DROPPED_CSV_COLUMNS: frozenset[str] = frozenset({"first_trade_id", "last_trade_id"})

DERIVED_COLUMNS: dict[str, str] = {"quote_qty": "price * qty (floor-precision exact for aggTrades)"}

# Spot timestamps switched to microseconds on 2025-01-01; earlier files are milliseconds.
TS_MICROSECOND_CUTOFF = "2025-01-01"


def ts_unit_for_date(date: str) -> str:
    """'us' for files dated on/after 2025-01-01, otherwise 'ms' (needs *1000 conversion)."""
    if date >= TS_MICROSECOND_CUTOFF:
        return "us"
    return "ms"


@dataclass(frozen=True)
class CanonicalSchema:
    version: int
    fields: tuple[tuple[str, str], ...]

    def names(self) -> tuple[str, ...]:
        return tuple(name for name, _ in self.fields)

    def polars_schema(self):
        import polars as pl

        dtype_map = {
            "int64": pl.Int64,
            "float64": pl.Float64,
            "string": pl.String,
            "bool": pl.Boolean,
        }
        return pl.Schema({name: dtype_map[t] for name, t in self.fields})


CANON = CanonicalSchema(version=SCHEMA_VERSION, fields=CANONICAL_FIELDS)
