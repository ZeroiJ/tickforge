import json
from pathlib import Path

import polars as pl
import pytest

from tickforge.normalize import ContractError, normalize_tick_csv, read_ticks_csv
from tickforge.schema import CANONICAL_NAMES, CANON

US_2026 = "2026-10-31"
MS_2024 = "2024-06-01"

# Headerless aggTrades CSV lines: agg_trade_id, price, qty, first, last, ts, is_buyer_maker, is_best_match
FIXTURE_ROWS_US = (
    "10,77368.76000000,0.01291000,90000,90000,1793404800022866,True,True\n"
    "11,77368.76000000,0.00106000,90001,90001,1793404800022957,False,False\n"
    "12,77369.10000000,0.50000000,90002,90002,1793404800024879,True,False\n"
)
FIXTURE_ROWS_MS_LEGACY = (
    "5,0.20000000,50.00000000,100,100,1717200001000,True,True\n"
    "6,0.20010000,25.00000000,101,101,1717200002000,False,False\n"
)
FIXTURE_ROWS_OUT_OF_ORDER = (
    "21,1.00000000,1.00000000,1,1,2000000000,True,True\n"
    "22,1.00000000,1.00000000,2,2,1000000000,False,False\n"
)
FIXTURE_ROWS_NEGATIVE = (
    "31,2.00000000,-0.50000000,1,1,3000000000,True,True\n"
)


def _fixture_csv(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "fixture.csv"
    p.write_text(content)
    return p


def _no_near_nulls(df: pl.DataFrame, *, names=None) -> None:
    for name in (names or CANONICAL_NAMES):
        assert df[name].null_count() == 0


CSV_MAPPED_COLUMNS = ["trade_id", "price", "qty", "ts", "is_buyer_maker", "is_best_match"]


class TestReadTicksCsv:
    def test_renames_drops_to_csv_mapped_fields(self, tmp_path):
        df = read_ticks_csv(_fixture_csv(tmp_path, FIXTURE_ROWS_US))
        assert df.columns == CSV_MAPPED_COLUMNS  # drops first/last trade_id provenance
        assert df["trade_id"].to_list() == [10, 11, 12]
        assert df["is_buyer_maker"].dtype == pl.Boolean
        assert df["is_best_match"].to_list() == [True, False, False]
        _no_near_nulls(df, names=CSV_MAPPED_COLUMNS)

    def test_final_output_is_exact_canonical_order(self, tmp_path):
        report = normalize_tick_csv(_fixture_csv(tmp_path, FIXTURE_ROWS_US), symbol="BTCUSDT", date=US_2026)
        df = pl.read_parquet(report["out_path"])
        assert df.columns == list(CANONICAL_NAMES)

    def test_quote_qty_is_derived_in_normalizer(self, tmp_path):
        report = normalize_tick_csv(_fixture_csv(tmp_path, FIXTURE_ROWS_US), symbol="BTCUSDT", date=US_2026, out_root=tmp_path)
        df = pl.read_parquet(report["out_path"])
        expected = [round(77368.76 * 0.01291, 12), round(77368.76 * 0.00106, 12), round(77369.10 * 0.5, 12)]
        assert df["quote_qty"].round(12).to_list() == pytest.approx(expected, abs=1e-9)

    def test_symbol_column_is_written(self, tmp_path):
        report = normalize_tick_csv(_fixture_csv(tmp_path, FIXTURE_ROWS_MS_LEGACY), symbol="btcusdt", date=MS_2024, out_root=tmp_path)
        df = pl.read_parquet(report["out_path"])
        assert set(df["symbol"].unique().to_list()) == {"BTCUSDT"}


class TestTsUnitLegacy:
    def test_pre_2025_files_convert_ms_to_us(self, tmp_path):
        report = normalize_tick_csv(_fixture_csv(tmp_path, FIXTURE_ROWS_MS_LEGACY), symbol="BTCUSDT", date=MS_2024, out_root=tmp_path)
        assert report["ts_unit"] == "ms"
        df = pl.read_parquet(report["out_path"])
        assert df["ts"].to_list() == [1_717_200_001_000_000, 1_717_200_002_000_000]

    def test_post_2025_files_stay_in_microseconds(self, tmp_path):
        report = normalize_tick_csv(_fixture_csv(tmp_path, FIXTURE_ROWS_US), symbol="BTCUSDT", date=US_2026, out_root=tmp_path)
        assert report["ts_unit"] == "us"
        df = pl.read_parquet(report["out_path"])
        assert df["ts"].to_list() == [1_793_404_800_022_866, 1_793_404_800_022_957, 1_793_404_800_024_879]


class TestCalloutChecks:
    def test_required_columns_have_no_nulls(self, tmp_path):
        report = normalize_tick_csv(_fixture_csv(tmp_path, FIXTURE_ROWS_US), symbol="BTCUSDT", date=US_2026)
        df = pl.read_parquet(report["out_path"])
        _no_near_nulls(df)  # all canonical columns incl. symbol/quote_qty

    def test_out_of_order_ts_fails_loudly(self, tmp_path):
        with pytest.raises(ContractError, match="contract violated"):
            normalize_tick_csv(_fixture_csv(tmp_path, FIXTURE_ROWS_OUT_OF_ORDER), symbol="BTCUSDT", date=US_2026)

    def test_non_positive_qty_fails_loudly(self, tmp_path):
        with pytest.raises(ContractError, match="contract violated"):
            normalize_tick_csv(_fixture_csv(tmp_path, FIXTURE_ROWS_NEGATIVE), symbol="BTCUSDT", date=US_2026)


class TestIdempotence:
    def test_second_run_writes_nothing(self, tmp_path):
        csv_path = _fixture_csv(tmp_path, FIXTURE_ROWS_US)
        first = normalize_tick_csv(csv_path, symbol="BTCUSDT", date=US_2026, out_root=tmp_path)
        assert first["rows_written"] == 3
        assert first["skipped"] is False

        second = normalize_tick_csv(csv_path, symbol="BTCUSDT", date=US_2026, out_root=tmp_path)
        assert second["rows_written"] == 0
        assert second["skipped"] is True

        # mutated csv => must NOT be treated as the same source
        csv_path.write_text(FIXTURE_ROWS_US + "13,77370.00000000,0.10000000,90003,90003,1793404800030000,True,True\n")
        third = normalize_tick_csv(csv_path, symbol="BTCUSDT", date=US_2026, out_root=tmp_path)
        assert third["skipped"] is False
        assert third["rows_written"] == 4

    def test_stats_sidecar_records_sha_and_ts_range(self, tmp_path):
        report = normalize_tick_csv(_fixture_csv(tmp_path, FIXTURE_ROWS_US), symbol="BTCUSDT", date=US_2026, out_root=tmp_path)
        stats = json.loads((Path(report["out_path"]).parent / "stats.json").read_text())
        assert stats["schema_version"] == CANON.version
        assert stats["rows"] == 3
        assert stats["ts_unit"] == "us"
        assert stats["min_ts"] == 1_793_404_800_022_866
        assert stats["max_ts"] == 1_793_404_800_024_879
        assert "csv_sha256" in stats
