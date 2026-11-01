import hashlib

import pytest

from tickforge.download import (
    build_checksum_url,
    build_zip_url,
    checksum_matches,
    expected_sha256_from_checksum_file,
)


class TestUrlBuilder:
    """Daily/monthly data.binance.vision URL contract (binance-public-data README)."""

    def test_builds_daily_aggtrades_url_for_full_date(self):
        url = build_zip_url(symbol="BTCUSDT", date="2026-10-31", interval="daily")
        assert url == (
            "https://data.binance.vision/data/spot/daily/aggTrades/"
            "BTCUSDT/BTCUSDT-aggTrades-2026-10-31.zip"
        )

    def test_builds_monthly_aggtrades_url_for_year_month(self):
        url = build_zip_url(symbol="BTCUSDT", date="2026-10", interval="monthly")
        assert url == (
            "https://data.binance.vision/data/spot/monthly/aggTrades/"
            "BTCUSDT/BTCUSDT-aggTrades-2026-10.zip"
        )

    def test_daily_interval_rejects_year_month(self):
        with pytest.raises(ValueError, match="daily"):
            build_zip_url(symbol="BTCUSDT", date="2026-10", interval="daily")

    def test_monthly_interval_rejects_full_date(self):
        with pytest.raises(ValueError, match="monthly"):
            build_zip_url(symbol="BTCUSDT", date="2026-10-31", interval="monthly")

    def test_rejects_unknown_interval(self):
        with pytest.raises(ValueError, match="interval"):
            build_zip_url(symbol="BTCUSDT", date="2026-10", interval="hourly")

    def test_symbol_is_uppercased(self):
        url = build_zip_url(symbol="btcusdt", date="2026-10-31", interval="daily")
        assert "/BTCUSDT/" in url

    def test_checksum_url_is_zip_url_plus_checksum_suffix(self):
        zip_url = build_zip_url(symbol="BTCUSDT", date="2026-10-31", interval="daily")
        assert build_checksum_url(zip_url) == zip_url + ".CHECKSUM"


class TestChecksum:
    """Matcher for the sha256sum-style .CHECKSUM files shipped next to every zip."""

    def test_reads_sha256_from_checksum_file_text(self):
        blob = hashlib.sha256(b"payload").hexdigest()
        assert expected_sha256_from_checksum_file(f"{blob}  file.zip") == blob
        # binance sometimes ships 'hash *<name>' / single-space variants
        assert expected_sha256_from_checksum_file(f"{blob} *file.zip") == blob
        assert expected_sha256_from_checksum_file(f"{blob} file.zip\n") == blob

    def test_rejects_malformed_checksum_text(self):
        with pytest.raises(ValueError, match="checksum"):
            expected_sha256_from_checksum_file("not-a-checksum")

    def test_checksum_matches_detects_correct_file(self, tmp_path):
        f = tmp_path / "small.bin"
        payload = b"agg_trade_id,price\n1,0.2\n"
        f.write_bytes(payload)
        assert checksum_matches(f, hashlib.sha256(payload).hexdigest())

    def test_checksum_matches_detects_corruption(self, tmp_path):
        f = tmp_path / "small.bin"
        f.write_bytes(b"agg_trade_id,price\n1,0.2\n")
        other = hashlib.sha256(b"different").hexdigest()
        assert not checksum_matches(f, other)

    def test_checksum_missing_file_is_not_a_match(self, tmp_path):
        assert not checksum_matches(tmp_path / "missing.bin", "0" * 64)
