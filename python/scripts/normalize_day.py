"""One-day pipeline entry: raw CSV -> canonical parquet + DuckDB verify.

    uv run python scripts/normalize_day.py --symbol BTCUSDT --date 2026-10-31

Exit code: 0 when the DuckDB cross-check passes (or a run was idempotently skipped).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # make `tickforge` importable from scripts/

import typer

from tickforge.download import repo_root
from tickforge.normalize import normalize_tick_csv
from tickforge.verify import verify_tick_file


def main(
    symbol: str = typer.Option("BTCUSDT", "--symbol"),
    date: str = typer.Option("2026-10-31", "--date"),
    kind: str = typer.Option("aggTrades", "--kind"),
) -> None:
    csv_path = repo_root / "data" / "raw" / f"{symbol.upper()}-{kind}-{date}.csv"
    if not csv_path.exists():
        print(f"missing raw csv: {csv_path} — run tickforge.download first", file=sys.stderr)
        raise typer.Exit(2)

    report = normalize_tick_csv(csv_path, symbol=symbol, date=date)
    print(json.dumps({"normalize": report}, indent=2))

    if not report["skipped"]:
        v = verify_tick_file(str(csv_path), report["out_path"])
        print(json.dumps({"duckdb_verify": v}, indent=2))
        if not v["ok"]:
            raise typer.Exit(1)


typer.run(main)
