"""Bulk download: monthly aggTrades zips for the Sprint-2 symbol set.

    uv run python scripts/bulk_download.py --symbols BTCUSDT ETHUSDT SOLUSDT BNBUSDT XRPUSDT --months 2026-09 2026-10

Uses tickforge.download.download_tick_file (idempotent + sha256 .CHECKSUM verified);
a second run is a no-op for every already-verified file. Prints one JSON line per
file so progress is greppable.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import typer

from tickforge.download import download_tick_file, repo_root


def main(
    symbols: list[str] = typer.Option(
        ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"],
        "--symbols", help="Symbols, repeated or comma-split."),
    months: list[str] = typer.Option(
        ["2026-09", "2026-10"], "--months"),
) -> None:
    # typer joins lists with the same option flag; be liberal in what we accept
    if len(symbols) == 1 and "," in symbols[0]:
        symbols = symbols[0].split(",")
    if len(months) == 1 and "," in months[0]:
        months = months[0].split(",")

    total_files = len(symbols) * len(months)
    print(f"downloading {total_files} monthly files: {symbols} x {months}", flush=True)
    for sym in symbols:
        for m in months:
            rep = download_tick_file(symbol=sym, date=m, interval="monthly")
            zip_mb = (Path(rep["zip_path"]).stat().st_size if Path(rep["zip_path"]).exists() else 0) / 1e6
            print(json.dumps({"symbol": sym, "month": m, "zip_mb": round(zip_mb, 1),
                              **{k: rep[k] for k in ("downloaded", "extracted", "csv_path", "elapsed_sec") if k in rep}}),
                  flush=True)


typer.run(main)
