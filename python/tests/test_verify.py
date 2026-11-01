from pathlib import Path

import pytest

from tickforge.normalize import normalize_tick_csv
from tickforge.verify import verify_tick_file

US_2026 = "2026-10-31"
ROWS = (
    "10,77368.76000000,0.01291000,90000,90000,1793404800022866,True,True\n"
    "11,77368.76000000,0.00106000,90001,90001,1793404800022957,False,False\n"
    "12,77369.10000000,0.50000000,90002,90002,1793404800024879,True,False\n"
)


def _setup(tmp_path: Path) -> tuple[Path, Path]:
    csv_path = tmp_path / "f.csv"
    csv_path.write_text(ROWS)
    report = normalize_tick_csv(csv_path, symbol="BTCUSDT", date=US_2026, out_root=tmp_path)
    return csv_path, Path(report["out_path"])


class TestVerifyTickFile:
    def test_normalized_parquet_agrees_with_raw_csv(self, tmp_path):
        csv_path, parquet_path = _setup(tmp_path)
        report = verify_tick_file(str(csv_path), str(parquet_path))
        assert report["ok"] is True
        assert report["csv"]["count"] == 3 == report["parquet"]["count"]
        # key subtlety: both probes read the RAW csv (c5 = ms-untouched) — dates >= 2025 are already µs, so equality holds
        assert report["csv"]["min_ts"] == report["parquet"]["min_ts"] == 1_793_404_800_022_866

    def test_mismatched_rows_fail_the_check(self, tmp_path):
        csv_path, parquet_path = _setup(tmp_path)
        csv_path.write_text(ROWS.splitlines()[0] + "\n")  # truncated source
        report = verify_tick_file(str(csv_path), str(parquet_path))
        assert report["ok"] is False

    def test_cli_exits_nonzero_on_mismatch(self, tmp_path):
        csv_path, parquet_path = _setup(tmp_path)
        csv_path.write_text(ROWS.splitlines()[0] + "\n")
        import subprocess, sys

        r = subprocess.run(
            [sys.executable, "-m", "tickforge.verify", str(csv_path), str(parquet_path)],
            capture_output=True,
            text=True,
        )
        assert r.returncode == 1
        assert '"ok": false' in r.stdout
