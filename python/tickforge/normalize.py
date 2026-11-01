"""CSV -> canonical parquet normalizer.

Pipeline (plan.md Phase 1): read headerless spot aggTrades CSV, rename to canonical
fields (contract in tickforge.schema), derive quote_qty, normalize ts -> microseconds,
write `data/norm/<symbol>/dt=<date>/trades.parquet` (+ stats.json sidecar).

Rules:
- Fail loudly on contract violation (nulls on fields, non-monotonic ts, non-positive price/qty).
- Idempotent: if the parquet already exists and the source csv sha256 matches the
  recorded one, the second run writes nothing (rows_written=0).
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import polars as pl

from tickforge.schema import (
    AGGTRADES_CSV_COLUMNS,
    CANONICAL_FIELDS,
    CSV_TO_CANONICAL,
    DROPPED_CSV_COLUMNS,
    DERIVED_COLUMNS,
    SCHEMA_VERSION,
    ts_unit_for_date,
)

repo_root = Path(__file__).resolve().parents[2]


class ContractError(RuntimeError):
    """Raised when untrusted input violates the canonical data contract."""


def _csv_dtype_map() -> type[pl.Schema] | pl.Schema:
    order = {}
    dtype = {"int64": pl.Int64, "float64": pl.Float64, "string": pl.String, "bool": pl.Boolean}
    for name, t in AGGTRADES_CSV_COLUMNS:
        order[name] = dtype[t]
    return pl.Schema(order)


def read_ticks_csv(csv_path: Path) -> pl.DataFrame:
    """Parse one headerless spot aggTrades CSV into a canonical-schema DataFrame.

    Source CSV is untrusted data (validate at the boundary). Types are pinned to the
    file spec; a malformed row should fail loudly here rather than corrupt the lake.
    """
    df = pl.read_csv(
        csv_path,
        has_header=False,
        new_columns=[name for name, _ in AGGTRADES_CSV_COLUMNS],
        schema_overrides=_csv_dtype_map(),
        low_memory=False,
    )
    df = df.drop(DROPPED_CSV_COLUMNS)
    df = df.rename(CSV_TO_CANONICAL)
    # NOTE: 'symbol'/'quote_qty' are derived downstream (normalize_tick_csv); canonical
    # field order is enforced there via _post() once every canonical column exists.
    return df


def _post(df: pl.DataFrame) -> pl.DataFrame:
    return df.select([name for name, _ in CANONICAL_FIELDS])


def _check_contract(df: pl.DataFrame, *, source: str) -> None:
    """Fail loudly: null keys, non-positive price/qty, ts non-decreasing within file."""
    viol = {}
    for name, _ in CANONICAL_FIELDS:
        nulls = df[name].null_count()
        if nulls:
            viol[f"nulls:{name}"] = nulls
    bad = df.filter((pl.col("price") <= 0) | (pl.col("qty") <= 0))
    if bad.height:
        viol["non_positive_price_qty"] = bad.height
    drift = df.select((pl.col("ts").diff() < 0).any())[0, 0] if df.height > 1 else False
    if drift:
        viol["ts_not_monotonic"] = True
    if viol:
        raise ContractError(f"{source}: contract violated: {viol}")


def normalize_tick_csv(
    csv_path: Path,
    *,
    date: str,
    symbol: str,
    out_root: Path | None = None,
    parquet_name: str = "trades.parquet",
) -> dict:
    """Normalize one day's aggTrades CSV into canonical parquet under out_root."""
    csv_path = Path(csv_path)
    out_root = Path(out_root) if out_root else repo_root / "data" / "norm"
    out_dir = out_root / symbol.lower() / f"dt={date}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / parquet_name
    stats_path = out_dir / "stats.json"

    csv_sha = hashlib.sha256(csv_path.read_bytes()).hexdigest()

    # Idempotency: source-of-truth = recorded csv hash; matching => run is a no-op.
    if stats_path.exists():
        prev = json.loads(stats_path.read_text())
        if prev.get("csv_sha256") == csv_sha and out_path.exists():
            return {"rows_written": 0, "skipped": True, "out_path": str(out_path)}

    ts_unit = ts_unit_for_date(date)
    df = read_ticks_csv(csv_path)
    if ts_unit == "ms":
        df = df.with_columns(pl.col("ts") * 1_000)  # legacy ms -> canonical microseconds

    # derive everything the canonical schema requires, THEN enforce the full contract
    df = df.with_columns(
        (pl.col("price") * pl.col("qty")).alias("quote_qty"),  # DERIVED (see schema.py)
        pl.lit(symbol.upper()).alias("symbol"),
    )
    df = _post(df)  # canonical field order
    _check_contract(df, source=csv_path.name)

    rows = df.height
    df.write_parquet(out_path, compression="zstd", statistics=True)

    stats = {
        "schema_version": SCHEMA_VERSION,
        "symbol": symbol.upper(),
        "date": date,
        "rows": rows,
        "csv_sha256": csv_sha,
        "csv_path": str(csv_path),
        "parquet_path": str(out_path),
        "ts_unit": ts_unit,
        "min_ts": int(df["ts"].min()),
        "max_ts": int(df["ts"].max()),
        "derived_columns": DERIVED_COLUMNS,
    }
    stats_path.write_text(json.dumps(stats, indent=2))
    return {
        "rows_written": rows,
        "skipped": False,
        "out_path": str(out_path),
        "min_ts": stats["min_ts"],
        "max_ts": stats["max_ts"],
        "ts_unit": ts_unit,
    }
