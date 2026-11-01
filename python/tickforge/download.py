"""Downloader for Binance public market data (data.binance.vision).

Contract (per plan.md Phase 1 + AGENTS.md):
- Idempotent: re-running must not re-download completed files (checksum-verified).
- Integrity first: every zip is verified against its sibling `.CHECKSUM` (sha256).
- Kill/resume friendly: downloads land in a `.part` file and are renamed atomically.

URL + column contract source:
  https://github.com/binance/binance-public-data (README.md)
  - spot daily files:  data/spot/daily/<kind>/<SYMBOL>/<SYMBOL>-<kind>-YYYY-MM-DD.zip
  - spot monthly:      data/spot/monthly/<kind>/<SYMBOL>/<SYMBOL>-<kind>-YYYY-MM.zip
  - spot timestamps from 2025-01-01 onwards are microseconds.
"""

from __future__ import annotations

import hashlib
import re
import time
import zipfile
from pathlib import Path

import httpx

DATA_BASE_URL = "https://data.binance.vision/data/spot"

# daily/aggTrades CSV is headerless, 8 columns (see README table for kind=aggTrades).
CHUNK_SIZE = 1024 * 1024
HTTP_TIMEOUT = httpx.Timeout(connect=30.0, read=120.0, write=60.0, pool=30.0)

_DAILY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_MONTHLY_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")

repo_root = Path(__file__).resolve().parents[2]


def build_zip_url(symbol: str, date: str, interval: str, kind: str = "aggTrades") -> str:
    sym = symbol.upper()
    if interval == "daily":
        if not _DAILY_RE.match(date):
            raise ValueError("interval=daily requires a YYYY-MM-DD date")
    elif interval == "monthly":
        if not _MONTHLY_RE.match(date):
            raise ValueError("interval=monthly requires a YYYY-MM date")
    else:
        raise ValueError(f"unknown interval {interval!r} (use 'daily' or 'monthly')")
    fname = f"{sym}-{kind}-{date}.zip"
    return f"{DATA_BASE_URL}/{interval}/{kind}/{sym}/{fname}"


def build_checksum_url(zip_url: str) -> str:
    return f"{zip_url}.CHECKSUM"


def expected_sha256_from_checksum_file(text: str) -> str:
    """Parse `<hex>  <name>` (or `*<name>`, newline-terminated) checksum text."""
    m = re.match(r"^([0-9a-fA-F]{64})(?:[ \t]+\*?.*)?$", text.strip())
    if not m:
        raise ValueError(f"malformed checksum text: {text.strip()[:80]!r}")
    return m.group(1).lower()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(CHUNK_SIZE), b""):
            h.update(chunk)
    return h.hexdigest()


def checksum_matches(path: Path, expected: str) -> bool:
    if not path.is_file():
        return False
    return sha256_file(path) == expected.lower()


def _pull_text(client: httpx.Client, url: str) -> str:
    resp = client.get(url, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    return resp.text


def _download_zip(client: httpx.Client, url: str, dest: Path) -> int:
    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_suffix(dest.suffix + ".part")
    written = 0
    with client.stream("GET", url, timeout=HTTP_TIMEOUT, follow_redirects=True) as resp:
        resp.raise_for_status()
        with part.open("wb") as fh:
            for chunk in resp.iter_bytes(CHUNK_SIZE):
                fh.write(chunk)
                written += len(chunk)
    part.replace(dest)  # atomic-ify: never leave a half zip at the canonical name
    return written


def _extract_zip(zip_path: Path, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as zf:
        names = zf.namelist()
        if len(names) != 1:
            raise RuntimeError(f"expected exactly 1 file in {zip_path.name}, got {names}")
        target = out_dir / names[0]
        if not target.is_file() or target.stat().st_size == 0:
            zf.extractall(out_dir)
        return target


def download_tick_file(
    symbol: str = "BTCUSDT",
    date: str = "2026-10-31",
    interval: str = "daily",
    kind: str = "aggTrades",
    data_dir: Path | None = None,
) -> dict:
    """Download (+verify +extract) one Binance data file into `data/raw/`.

    Second run with the same args must be a no-op (returns skipped=True).
    """
    data_dir = Path(data_dir) if data_dir else repo_root / "data" / "raw"
    data_dir.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()

    zip_url = build_zip_url(symbol, date, interval, kind)
    zip_path = data_dir / zip_url.rsplit("/", 1)[-1]
    checksum_path = zip_path.with_suffix(zip_path.suffix + ".CHECKSUM")
    report = {"zip_url": zip_url, "zip_path": str(zip_path), "downloaded": False, "extracted": False}

    client = httpx.Client(headers={"User-Agent": "tickforge/0.1"})
    try:
        # 1) checksum file (tiny; always the source of truth, refreshed when missing)
        if not checksum_path.is_file():
            checksum_path.write_text(_pull_text(client, build_checksum_url(zip_url)))
        expected = expected_sha256_from_checksum_file(checksum_path.read_text())

        # 2) zip: skip if already present and valid; else stream to .part and rename
        if checksum_matches(zip_path, expected):
            report["downloaded"] = False  # idempotent skip
        else:
            bytes_written = _download_zip(client, zip_url, zip_path)
            if not checksum_matches(zip_path, expected):
                zip_path.unlink(missing_ok=True)
                raise RuntimeError(
                    f"sha256 mismatch after download for {zip_path.name} — deleted; rerun to retry"
                )
            report.update(downloaded=True, bytes_written=bytes_written)
    finally:
        client.close()

    # 3) extract (idempotent: skip if the CSV already landed)
    csv_path = _extract_zip(zip_path, data_dir)
    report["extracted"] = True
    report["csv_path"] = str(csv_path)
    report["elapsed_sec"] = round(time.perf_counter() - started, 1)
    return report


if __name__ == "__main__":
    import json

    import typer

    def main(
        symbol: str = typer.Option("BTCUSDT", "--symbol"),
        date: str = typer.Option("2026-10-31", "--date"),
        interval: str = typer.Option("daily", "--interval"),
        kind: str = typer.Option("aggTrades", "--kind"),
    ) -> None:
        rep = download_tick_file(symbol, date, interval, kind)
        print(json.dumps(rep, indent=2))

    typer.run(main)
