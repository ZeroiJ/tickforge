"""Cross-check canonical parquet vs raw CSV via DuckDB.

Per AGENTS.md §4 (Python rules): always cross-check counts with DuckDB.
DuckDB is the correctness oracle only — never canonical storage (ADR-0001).

`ok=True` when csv rows == parquet rows, qty totals agree (f64-tolerant),
price extremes + ts range + max trade id are exact matches.
"""

from __future__ import annotations

import duckdb

_KEYS = ("count", "min_trade_id", "max_trade_id", "min_price", "max_price", "sum_qty", "min_ts", "max_ts")

_CSV_TYPES = (
    ("c0", "BIGINT"),  # agg_trade_id
    ("c1", "DOUBLE"),  # price
    ("c2", "DOUBLE"),  # qty
    ("c3", "BIGINT"),  # first_trade_id (dropped at normalize; headerless width must match)
    ("c4", "BIGINT"),  # last_trade_id
    ("c5", "BIGINT"),  # ts (raw unit! CSV keeps native units; verifier compares raw-vs-raw)
    ("c6", "BOOLEAN"),  # is_buyer_maker
    ("c7", "BOOLEAN"),  # is_best_match
)


def verify_tick_file(csv_path: str, parquet_path: str) -> dict:
    con = duckdb.connect()
    try:
        colspec = ", ".join(f"'{n}':'{t}'" for n, t in _CSV_TYPES)
        csv_row = con.execute(
            f"SELECT COUNT(*), MIN(c0), MAX(c0), MIN(c1), MAX(c1), SUM(c2), MIN(c5), MAX(c5) "
            f"FROM read_csv(?, header=false, columns={{{colspec}}})",
            (csv_path,),
        ).fetchone()
        pq_row = con.execute(
            "SELECT COUNT(*), MIN(trade_id), MAX(trade_id), MIN(price), MAX(price), SUM(qty), MIN(ts), MAX(ts) "
            "FROM read_parquet(?)",
            (parquet_path,),
        ).fetchone()
    finally:
        con.close()

    s_csv: dict[str, object] = dict(zip(_KEYS, csv_row))
    s_pq: dict[str, object] = dict(zip(_KEYS, pq_row))
    s_pq["sum_qty"] = round(float(s_pq["sum_qty"]), 9)  # f64 tolerance

    tol = abs(float(s_csv["sum_qty"])) * 1e-9 + 1e-9
    ok = (
        s_csv["count"] == s_pq["count"]
        and abs(float(s_csv["sum_qty"]) - float(s_pq["sum_qty"])) <= tol
        and s_csv["min_ts"] == s_pq["min_ts"]
        and s_csv["max_ts"] == s_pq["max_ts"]
        and s_csv["min_price"] == s_pq["min_price"]
        and s_csv["max_price"] == s_pq["max_price"]
        and s_csv["max_trade_id"] == s_pq["max_trade_id"]
    )
    return {"ok": bool(ok), "csv": s_csv, "parquet": s_pq}


if __name__ == "__main__":
    import json
    import sys

    if len(sys.argv) != 3:
        print("usage: python -m tickforge.verify <csv_path> <parquet_path>")
        sys.exit(2)
    report = verify_tick_file(sys.argv[1], sys.argv[2])
    print(json.dumps(report, indent=2))
    sys.exit(0 if report["ok"] else 1)
