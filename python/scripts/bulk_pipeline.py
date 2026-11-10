"""Bulk pipeline for the Sprint-2 set: raw monthly CSV -> canonical parquet ->
DuckDB verify -> ingest into .mkt.

    uv run python scripts/bulk_pipeline.py --symbols ... --months ...

Per file (idempotent at both normalize and ingest layers):
  1. normalize_tick_csv (raw CSV -> data/norm/<sym>/dt=<month>/trades.parquet)
  2. verify_tick_file (DuckDB cross-check vs raw CSV — oracle, ADR-0001)
  3. tf-cli ingest-norm (parquet -> data/mkt/<sym>/dt=<month>/trades.mkt)

One JSON line per file; exit nonzero when any DuckDB verify fails.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import subprocess
import typer

from tickforge.download import repo_root
from tickforge.normalize import normalize_tick_csv
from tickforge.verify import verify_tick_file


def ingest_cli(parquet: Path) -> str:
    exe = next((repo_root / "rust" / "target" / profile / "tf-cli.exe" for profile in
                ("release", "debug") if (repo_root / "rust" / "target" / profile / "tf-cli.exe").exists()))
    proc = subprocess.run(
        [str(exe), "ingest-norm", "--input", str(parquet)],
        capture_output=True, text=True, timeout=3600,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip())
    return proc.stdout.strip()


def main(
    symbols: list[str] = typer.Option(
        ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"], "--symbols"),
    months: list[str] = typer.Option(["2026-09", "2026-10"], "--months"),
) -> None:
    if len(symbols) == 1 and "," in symbols[0]:
        symbols = symbols[0].split(",")
    if len(months) == 1 and "," in months[0]:
        months = months[0].split(",")

    any_fail = False
    for sym in symbols:
        for m in months:
            csv = repo_root / "data" / "raw" / f"{sym}-aggTrades-{m}.csv"
            if not csv.exists():
                print(json.dumps({"symbol": sym, "month": m, "error": f"missing {csv}"}), flush=True)
                any_fail = True
                continue
            rep = normalize_tick_csv(csv, symbol=sym, date=m)
            row = {"symbol": sym, "month": m,
                   "rows": rep.get("rows_written", 0), "skipped": rep.get("skipped", False)}
            if not rep.get("skipped"):
                v = verify_tick_file(str(csv), str(rep["out_path"]))
                row["duckdb_ok"] = bool(v["ok"])
                if not v["ok"]:
                    any_fail = True
                    row["duckdb"] = v
            parquet = rep["out_path"] or (
                repo_root / "data" / "norm" / sym.lower() / f"dt={m}" / "trades.parquet")
            ing = ingest_cli(parquet)
            row["ingest"] = ing
            print(json.dumps(row, default=str), flush=True)

    raise typer.Exit(1 if any_fail else 0)


typer.run(main)
