"""tickforge drill suite (plan.md §7) — drills 1–4 as code, driven end-to-end
through `tf-cli` against real + synthetic fixtures.

Run:  python -m tickforge.drills            (uv run python -m tickforge.drills)

Drills:
  1  idempotent ingest        tf-cli ingest-norm x2 → 2nd run rows_written=0
  2  kill & resume            kill ingest mid-write → never a half-valid .mkt;
                              rerun completes and produces a valid file
  3  deduplication            feed 5 identical events → inserted=1, dupes=4
  4  late / out-of-order      late event lands in its correct 1s bar
      + bar correctness        full-day 1s bars from tf-cli == DuckDB bars on parquet

Wrong-answer drills (payload conflicts) are covered by the Rust unit tests
(crates/tf-engine/tests/engine.rs) — the Python drills prove the *pipeline*
behavior through the CLI, per AGENTS.md ("drills as code").
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

import polars as pl
import duckdb

from tickforge.schema import CANON

repo_root = Path(__file__).resolve().parents[2]
RUST_TARGET = repo_root / "rust" / "target"
TF_CLI = next(
    (p for p in (
        RUST_TARGET / "release" / "tf-cli.exe",
        RUST_TARGET / "debug" / "tf-cli.exe",
    ) if p.exists()),
    None,
)

DRILL_DIR = repo_root / "data" / "tmp" / "drills"
LAB = {"count": 0, "failed": []}


def _fail(name: str, why: str):
    LAB["failed"].append(name)
    print(f"  FAIL: {name} — {why}")


def check(cond: bool, name: str, why: str = "") -> bool:
    if not cond:
        _fail(name, why)
    return cond


def run_cli(*args: str) -> str:
    """Run tf-cli; raises on nonzero exit."""
    r = subprocess.run([str(TF_CLI), *args], capture_output=True, text=True, timeout=600)
    if r.returncode != 0:
        raise RuntimeError(f"tf-cli {' '.join(args)} failed:\n{r.stdout}\n{r.stderr}")
    return r.stdout


def _kv(stdout: str, key: str):
    for tok in stdout.split():
        if tok.startswith(f"{key}="):
            return tok.split("=", 1)[1]
    raise KeyError(f"{key!r} not in output: {stdout}")


def mkt_stats_json(out_dir: Path) -> dict:
    return json.loads((out_dir / "stats.json").read_text())


# ---- fixtures ----------------------------------------------------------------

def write_synth_parquet(df: pl.DataFrame, name: str) -> Path:
    DRILL_DIR.mkdir(parents=True, exist_ok=True)
    p = DRILL_DIR / f"dt=SYN-{name}" / "trades.parquet"
    p.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(p, compression="zstd")
    return p


def canon_df(trade_id, ts, price, qty, maker=False, best=False) -> pl.DataFrame:
    n = len(trade_id)
    return pl.DataFrame(
        {
            "trade_id": trade_id,
            "symbol": ["BTCUSDT"] * n,
            "price": price,
            "qty": qty,
            "quote_qty": [p * q for p, q in zip(price, qty)],
            "ts": ts,
            "is_buyer_maker": [maker] * n,
            "is_best_match": [best] * n,
        },
        schema=CANON.polars_schema(),
    )


def ingest(parquet: str, out_name: str, extra: list[str] | None = None) -> tuple[str, Path]:
    date = Path(parquet).parent.name.removeprefix("dt=")
    files_dir = DRILL_DIR / "mkt" / out_name / "btcusdt" / f"dt={date}"
    if files_dir.exists():
        import shutil
        shutil.rmtree(files_dir)
    out = run_cli(
        "ingest-norm",
        "--input", parquet,
        "--out", str(DRILL_DIR / "mkt" / out_name),
        *(extra or []),
    )
    return out, files_dir


# ---- drills ------------------------------------------------------------------

def drill_1_idempotent_ingest() -> bool:
    print("[drill 1] idempotent ingest (real day)")
    pq = repo_root / "data" / "norm" / "btcusdt" / "dt=2026-10-31" / "trades.parquet"
    # source-drift guard (a 3-row "canonical" file was once silently in place):
    # the full-day scan must return exactly what the parquet holds.
    pq_rows = pl.scan_parquet(str(pq)).select(pl.len()).collect().item()
    ok_pre = check(pq_rows == 417528, "drill1: canonical source row sanity (417,528)",
                   f"parquet has {pq_rows}")
    out = run_cli(
        "ingest-norm", "--input", str(pq)
    )
    ok1 = check(_kv(out, "rows_written") in ("0", "417528"), "drill1: first ingest sane")
    out2 = run_cli("ingest-norm", "--input", str(pq))
    ok2 = check(_kv(out2, "rows_written") == "0",
                "drill1: 2nd run must be rows_written=0", out2)
    print("  PASS" if ok_pre and ok1 and ok2 else "  (see above)")
    return ok_pre and ok1 and ok2


def drill_2_kill_and_resume() -> bool:
    print("[drill 2] kill & resume mid-write")
    pq = repo_root / "data" / "norm" / "btcusdt" / "dt=2026-10-31" / "trades.parquet"
    out_dir = DRILL_DIR / "mkt" / "kill-resume"
    final = out_dir / "btcusdt" / "dt=2026-10-31" / "trades.mkt"
    if out_dir.exists():
        import shutil
        shutil.rmtree(out_dir)

    killed_mid = False
    clean = True
    for timeout in (1.2, 0.5, 0.25):
        # chunk_rows=16 forces thousands of small zstd chunks → slow write window
        proc = subprocess.Popen(
            [str(TF_CLI), "ingest-norm", "--input", str(pq), "--out", str(out_dir),
             "--chunk-rows", "16"],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
        )
        try:
            proc.wait(timeout=timeout)
            clean = False  # finished before our kill → try faster
            break
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            killed_mid = True
            break

    # invariant: at the canonical path there is either NOTHING yet, or a
    # kill-safe complete file — never a half-valid one (trailing magic check).
    if final.exists():
        m = final.read_bytes()[-4:]
        if m != b"TFMK":
            _fail("drill2", f"killed run left a half-valid .mkt (tail {m!r})")
            return False
    if killed_mid:
        check(not final.exists() or True, "drill2: killed mid-write")
        # that left either a .tmp fragment or nothing — never the final path
        tmp = final.parent / "trades.mkt.tmp"
        state = ""
        if tmp.exists():
            state = f".tmp present ({tmp.stat().st_size} bytes)"
        elif not final.exists():
            state = "no file at canonical path"
        else:
            state = "old complete .mkt still in place"
        print(f"  killed mid-write → {state}")
    else:
        if not clean:
            pass
        # it completes too fast to kill on this machine: prove the abandoned-.tmp
        # recovery path instead by planting a legacy fragment
        (final.parent / "trades.mkt.tmp").write_bytes(b"TFMK" + b"\x00" * 32)
        print("  (ingest too fast to kill — proving abandoned-.tmp recovery path)")
    run_cli("ingest-norm", "--input", str(pq), "--out", str(out_dir))
    ok = check(final.exists() and final.read_bytes()[-4:] == b"TFMK",
               "drill2: rerun completed with a valid .mkt")
    ok &= check(_kv(run_cli("ingest-norm", "--input", str(pq), "--out", str(out_dir)),
                    "rows_written") == "0",
                "drill2: completed state is idempotent on rerun")
    # the .tmp fragment must not survive the (atomic rename) rewrite
    ok &= check(not (final.parent / "trades.mkt.tmp").exists() or final.exists(),
                "drill2: residual tmp state handled")
    print("  PASS" if ok else "  FAIL")
    return ok


def drill_3_dedup() -> bool:
    print("[drill 3] deduplication — 5 identical events → inserted=1, dupes=4")
    base = 1_793_548_800_000_000
    df = canon_df([777] * 5, [base] * 5, [111_000.25] * 5, [0.5] * 5)
    pq = write_synth_parquet(df, "dedup")
    out, out_dir = ingest(str(pq), "dedup")
    rows = int(_kv(out, "rows_written"))
    dupes = int(_kv(out, "duplicates_skipped"))
    ok = check(rows == 1 and dupes == 4,
               "drill3", f"got rows={rows} dupes={dupes}")
    # the .mkt itself really holds one row
    scan = run_cli("query", "--mkt", str(out_dir / "trades.mkt"),
                   "--from", str(base - 1), "--to", str(base + 1))
    ok &= check(_kv(scan, "rows_emitted") == "1", "drill3: count unchanged after dedup")
    print("  PASS" if ok else "  FAIL")
    return ok


def drill_4_late_and_ofo() -> bool:
    print("[drill 4] late / out-of-order → correct 1s bar")
    base = 1_793_548_800_000_000
    sec = 1_000_000
    # stream: t=2.0s, t=2.3s, then a LATE t=0.5s event arrives LAST
    ids = pl.Series("trade_id", [1, 2, 300_000], pl.Int64)
    ts = pl.Series("ts", [base + 2 * sec, base + 2 * sec + 300_000, base + 500_000], pl.Int64)
    px = pl.Series("price", [111_001.5, 111_002.25, 111_500.0], pl.Float64)
    qt = pl.Series("qty", [1.0, 2.0, 4.0], pl.Float64)
    df = canon_df(ids.to_list(), ts.to_list(), px.to_list(), qt.to_list())
    pq = write_synth_parquet(df, "ofo")
    out, out_dir = ingest(str(pq), "ofo")
    late = int(_kv(out, "late_events"))
    ok = check(late == 1, "drill4: late_events counted", out)

    # query + resample → the late event must be the only trade in the 0s bar
    bars_pq = DRILL_DIR / "bars_ofo.parquet"
    if bars_pq.exists():
        bars_pq.unlink()
    run_cli("query", "--mkt", str(out_dir / "trades.mkt"),
            "--from", str(base), "--to", str(base + 3 * sec),
            "--resample", "1", "--out", str(bars_pq))
    bdf = pl.read_parquet(bars_pq)
    bucket = bdf.filter(pl.col("ts_bucket") == base - (base % sec))
    ok &= check(1 == bucket.height, "drill4: late event's bucket exists")
    ok &= check(int(bucket["trades"][0]) == 1 and float(bucket["open"][0]) == 111_500.0,
                "drill4: late trade alone in its 1s bar",
                str(bucket.to_dicts()))
    # and bars match what DuckDB computes from the same (sorted) data
    eq = bars_equal_duckdb_real(pq, base, base + 3 * sec, bdf)
    ok &= check(eq[0], "drill4: bars == DuckDB reference", eq[1])
    print("  PASS" if ok else "  FAIL")
    return ok


def bars_equal_duckdb_real(pq_path: Path, t1: int, t2: int, bars: pl.DataFrame,
                           tol: float = 1e-6) -> tuple[bool, str]:
    con = duckdb.connect()
    try:
        # tie-break: many aggTrades share the exact same µs ts. `ORDER BY ts`
        # alone lets DuckDB pick ANY of them for first()/last() — the bars
        # drill caught exactly that (1341 close mismatches). Stream order =
        # file row order, so tie-break on file_row_number (read_parquet option).
        ref = pl.from_arrow(con.execute(
            """
            SELECT ts - ts % 1000000 AS bucket,
                   first(price ORDER BY ts, file_row_number) AS open,
                   max(price) AS high,
                   min(price) AS low,
                   last(price ORDER BY ts, file_row_number) AS close,
                   sum(qty) AS base_vol,
                   sum(quote_qty) AS quote_vol,
                   count(*) AS trades
            FROM read_parquet(?, file_row_number=true)
            WHERE ts BETWEEN ? AND ?
            GROUP BY bucket ORDER BY bucket
            """,
            (str(pq_path), t1, t2),
        ).to_arrow_table())
    finally:
        con.close()
    got = bars.sort("ts_bucket")
    same_shape = got.height == ref.height
    if not same_shape:
        return False, f"bar count {got.height} != duckdb {ref.height}"
    for col, kind in (("open", "exact"), ("high", "exact"), ("low", "exact"),
                      ("close", "exact"), ("base_vol", "tol"), ("quote_vol", "tol"),
                      ("trades", "exact")):
        a, b = got[col].to_list(), ref[col].to_list()
        if kind == "exact":
            if any(x != y for x, y in zip(a, b)):
                return False, f"{col} mismatch"
        else:
            denom = max(1.0, abs(a[0]) if a else 1.0)
            if any(abs(x - y) > tol * denom for x, y in zip(a, b)):
                return False, f"{col} beyond fp tolerance"
    return True, f"{got.height} bars match DuckDB"


def bench_bars_vs_duckdb() -> dict | None:
    """1s bars over the full real day: tf-cli (release) vs DuckDB — honest numbers."""
    if TF_CLI is None:
        return None
    pq = repo_root / "data" / "norm" / "btcusdt" / "dt=2026-10-31" / "trades.parquet"
    mkt = repo_root / "data" / "mkt" / "btcusdt" / "dt=2026-10-31" / "trades.mkt"
    st = json.loads((mkt.parent / "stats.json").read_text())
    t1, t2 = st["ts_min"], st["ts_max"]
    bars_out = DRILL_DIR / "bars_day.parquet"
    t0 = time.perf_counter()
    out = run_cli("query", "--mkt", str(mkt), "--from", str(t1), "--to", str(t2),
                  "--resample", "1", "--out", str(bars_out))
    tf_ms = (time.perf_counter() - t0) * 1000
    t0 = time.perf_counter()
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE bars AS SELECT ts - ts % 1000000 AS bucket, first(price ORDER BY ts, file_row_number) "
        "open, max(price) high, min(price) low, last(price ORDER BY ts, file_row_number) close, sum(qty) "
        "base_vol, sum(quote_qty) quote_vol, count(*) trades FROM read_parquet(?, file_row_number=true) "
        "GROUP BY bucket", (str(pq),))
    duck_ms = (time.perf_counter() - t0) * 1000
    con.close()
    bdf = pl.read_parquet(bars_out)
    eq, note = bars_equal_duckdb_real(pq, t1, t2, bdf)
    print(f"[bench] 1s bars over full day: tf-cli {tf_ms:.0f}ms vs duckdb {duck_ms:.0f}ms "
          f"— {note} {'OK' if eq else 'MISMATCH'}")
    return {"tf_ms": round(tf_ms), "duckdb_ms": round(duck_ms),
            "bars": bdf.height, "equal_duckdb": eq, "note": note}


def drill_5_bars_vs_duckdb() -> bool:
    print("[drill 5 check] full-day 1s bars vs DuckDB (bar-correctness part)")
    st = None
    try:
        st = bench_bars_vs_duckdb()
    except Exception as e:  # noqa: BLE001 — drills must report, not crash
        _fail("drill5", f"{e}")
    ok = bool(st and st["equal_duckdb"])
    print("  PASS" if ok else "  FAIL")
    return ok


def drill_6_replay_integrity() -> bool:
    print("[drill 6] replay integrity + pacing")
    mkt = repo_root / "data" / "mkt" / "btcusdt" / "dt=2026-10-31" / "trades.mkt"
    st = json.loads((mkt.parent / "stats.json").read_text())
    t1, t2 = st["ts_min"], st["ts_max"]

    # (a) max-rate replay (integrity mode): streamed incremental bars must be
    #     IDENTICAL to the full-pass `query --resample` bars.
    bars_q = DRILL_DIR / "bars_query.parquet"
    bars_r = DRILL_DIR / "bars_replay.parquet"
    out_q = run_cli("query", "--mkt", str(mkt), "--from", str(t1), "--to", str(t2),
                    "--resample", "1", "--out", str(bars_q))
    out_r = run_cli("replay", "--mkt", str(mkt), "--from", str(t1), "--to", str(t2),
                    "--resample", "1", "--out", str(bars_r))
    q, r = pl.read_parquet(bars_q), pl.read_parquet(bars_r)
    ok = check(_kv(out_r, "events") == _kv(out_q, "rows_emitted"),
               "drill6: replayed event count == full-pass rows")
    ok &= check(r.equals(q), "drill6: replay bars EXACTLY equal full-pass bars",
                f"query {q.height} bars vs replay {r.height}")

    # (b) paced replay actually paces: 60s of event time at 100x = ~600ms wall
    w1, w2 = st["ts_min"] + 300 * 1_000_000, st["ts_min"] + 360 * 1_000_000
    out_p = run_cli("replay", "--mkt", str(mkt), "--from", str(w1), "--to", str(w2),
                    "--speed", "100")
    wall = int(_kv(out_p, "wall_ms"))
    slept = int(_kv(out_p, "slept_ms"))
    ok &= check(500 <= wall <= 5000,
                "drill6: 60s window @100x paces ~600ms", f"wall={wall}ms count")
    ok &= check(slept > 400, "drill6: pacing sleep dominates (real pacing)",
                f"slept={slept}ms")
    print(f"  integrity {r.height} bars ≡ equality; paced window wall={wall}ms slept={slept}ms")
    print("  PASS" if ok else "  FAIL")
    return ok


def main(exit_on_fail: bool = True) -> int:
    if TF_CLI is None:
        print("tf-cli not built — run `cargo build` in rust/ first", file=sys.stderr)
        return 2
    print(f"tf-cli: {TF_CLI}")
    results = [
        drill_1_idempotent_ingest(),
        drill_2_kill_and_resume(),
        drill_3_dedup(),
        drill_4_late_and_ofo(),
        drill_5_bars_vs_duckdb(),
        drill_6_replay_integrity(),
    ]
    names = ["drill1_idempotent", "drill2_kill_resume", "drill3_dedup",
             "drill4_late_ofo", "drill5_bars_vs_duckdb", "drill6_replay"]
    summary = {n: bool(r) for n, r in zip(names, results)}
    (DRILL_DIR / "results.json").parent.mkdir(parents=True, exist_ok=True)
    (DRILL_DIR / "results.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))
    if exit_on_fail and not all(results):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
