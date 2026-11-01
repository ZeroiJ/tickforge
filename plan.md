# `tickforge` Plan: From Zero -> Resume Worthy

Goal: Build a **tick-level crypto market microstructure lab** in Rust + Python + R. Not just "download data", but build your own columnar engine that can query 100GB of tick data with only 15GB of RAM, prove it works, and make it look like something you'd see on a real data engineering portfolio.

Your laptop is perfect for this. 476GB disk with ~267GB free and 15GB RAM is more than enough *if* you treat RAM as a constraint on purpose.


## 1) Why this over everything else

Looking at your existing repos, you already did batch lakes, Airflow, dbt, and fraud detection. What you're missing is **high-frequency + low-level systems + analytics** all in one project.

`tickforge` hits almost every box recruiters look for from a CSE(DS) student aiming for Data/Analytics/DE roles:

| Skill | How `tickforge` shows it |
|---|---|
| **Low-level Systems (Rust)** | Write your own binary columnar format (`.mkt`), memory-mapped reads, zero-copy style parsing. |
| **Data Engineering** | Ingestion, deduplication, schema contracts, idempotency, replayability (same ideas as `p1-market-data-lake`) |
| **Market Data** | Trades, aggTrades, orderbook. Teaches timestamps, out-of-order, duplicates, gaps. Far more realistic than CSV cleaning. |
| **Analytical Engineering** | Normalize to Parquet/Delta, model with dbt later and push to a lakehouse. |
| **Statistics/Research (R)** | Prove a real microstructure hypothesis with graphs, not "I plotted a heatmap". |
| **Cloud/Lakehouse Ready** | Make it compatible with **Databricks Volumes/Unity Catalog/Delta Lake** so you can say you touched real world lakehouse tooling. |


## 2) Research Takeaways (What I looked up)

After going through the public repos/formats I found, here's what actually matters for this project.

| Reference | Takeaway for `tickforge` |
|---|---|
| [`binance/binance-public-data`](https://github.com/binance/binance-public-data) | Use `data.binance.vision`. Spot `trades/aggTrades` are perfect to start. Timestamps moved to microseconds in 2025+, keep that in mind. |
| [`zhiquanlao/binance_data_collector`](https://github.com/zhiquanlao/binance_data_collector) | Shows the correct mental model: separate "collector" (Rust) from "orchestrator" (Python). Also good ideas about writing tick-level data without blocking. |
| [`Mattbusel/crypto-orderbook`](https://github.com/Mattbusel/crypto-orderbook) | Teaches snapshot + diff + sequencing for orderbooks. **You do NOT need to replicate this fully day 1.** Just understand why gap detection/resync exists. |
| [`snapevent`](https://github.com/PCeltide/snapevent) | One of the cleanest references. Uses raw JSONL -> processed columnar, deterministic replay, and a Python access layer. Steal this philosophy, don't copy 1:1. |
| [`MG-Trading-Terminal/mg-exchange-db`](https://github.com/MG-Trading-Terminal/mg-exchange-db) | Inspiring, but way too big. The lesson: **partitioning + columnar + predicate pushdown > loading everything at once**. |
| [Vortex Columnar Format](https://dfa1.github.io/articles/vortex-on-a-real-dataset.html) | Reinforces the exact idea you want. Don't just gzip parquet, think in terms of *per-column encodings*. Even a dumb version of this will teach you more than using `pyarrow` blindly. |
| [Crypto Lake](https://crypto-lake.com/data/) | Great to see how real vendors structure `trades/book/candles` by `exchange/symbol/dt=`. Mirror this layout on disk. |


## 3) Stack & Roles

Keep each language doing what it's actually good at. This is important to justify on your resume.

| Tech | Role | Notes |
|---|---|---|
| **[Rust](https://www.rust-lang.org/)** | Core Engine | WebSocket/HTTP, binary reader/writer, `memmap2`, `zstd`, column pruning by time. This is your "flagship". |
| **[Python](https://www.python.org/)** | Glue & Automation | Download from `data.binance.vision`, normalize schemas, run drills, orchestrate with `typer` CLI, convert to Parquet/Delta. |
| **[R](https://www.r-project.org/)** | Quant Research | `data.table`, `zoo/xts`, `ggplot2`. Run microstructure tests (realized volatility, orderflow imbalance vs short returns). |
| **[DuckDB](https://duckdb.org/)** | Sanity checker | Use it to verify your custom engine is correct. Don't use it as a crutch. |
| **[Polars](https://www.pola.rs/)** | Fast EDA | Great for quick checks between Rust runs. |
| **[Databricks](https://www.databricks.com/)** | Lakehouse exposure | Upload normalized data to **Databricks Volumes**, convert to **Delta Lake**, query with SQL, maybe make a tiny dashboard. Free Community Edition is more than enough. |
| **[dbt](https://www.getdbt.com/)** | Modeling (optional but nice) | If you want to flex your existing `flight_dbt` skills, model `trades` -> `bars` -> `features` in dbt Core against DuckDB/Databricks. |


## 4) Folder Structure

Keep this clean from day 1. It'll help a ton when using an agent.

```text
C:\Projects\tickforge\
├── README.md        # The "proof" document (readme-driven development)
├── plan.md          # This plan
├── docs/            # ADRs, format spec, notes
│   ├── ADR-0001-custom-mkt-format.md
│   ├── ADR-0002-snapshot-diff-orderbook.md
│   └── format-spec.md
├── rust/            # cargo workspace
│   ├── Cargo.toml
│   ├── crates/
│   │   ├── tf-core/      # types, time, schema, hashing
│   │   ├── tf-collect/   # downloader + ws collector (later)
│   │   ├── tf-format/    # .mkt writer/reader + compression
│   │   └── tf-engine/    # scan, filter by time range, resample, dedup
│   └── bin/
│       └── tf-cli/       # unified CLI (ingest/query/replay/bench)
├── python/          # orchestration & drills
│   ├── pyproject.toml
│   ├── tickforge/
│   │   ├── download.py  # pull from data.binance.vision
│   │   ├── normalize.py # CSV -> canonical schema
│   │   ├── verify.py    # cross-check vs DuckDB
│   │   └── drills.py    # kill/restart, dupes, late data tests
│   └── scripts/
├── r/               # research
│   ├── 01_eda.R
│   ├── 02_microstructure.R
│   └── renv.lock (or just list packages)
├── data/            # gitignored
│   ├── raw/         # zip/csv from binance.vision
│   ├── norm/        # canonical parquet
│   ├── mkt/         # your custom .mkt files
│   └── lake/        # delta/partitioned for databricks
└── docker/          # optional later (redpanda/clickhouse) NOT required now
```

> Add `data/`, `*.mkt`, `*.parquet` to `.gitignore`. You want to show *code*, not 100GB of binaries.


## 5) Data Plan (Free & Feasible)

Don't start with "everything". Start with a dataset you can reason about end-to-end.

| Stage | Symbols | Range | Type | Size (approx) | Goal |
|---|---|---|---|---|---|
| **Sprint 0** | `BTCUSDT` | 1 day | `aggTrades` or `trades` | ~150-400 MB raw CSV/zip | Parse, normalize, write/read with Rust. Must finish in <1 hour. |
| **Sprint 1** | `BTCUSDT`, `ETHUSDT` | 7 days | `aggTrades` | ~2-5 GB | Test deduplication, time-ordering, out-of-order handling. |
| **Sprint 2** | Top 5 (`BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT`) | 30-90 days | `aggTrades` | ~15-40 GB raw | Build resampling (1s/1m), features, and your first benchmark. |
| **Final Target** | Top 8-10 | 3-6 months | `aggTrades` + `trades` | **~70-120 GB raw -> ~12-25 GB `.mkt`** | Hit your "50% of disk" feel without filling RAM. Query full range fast. |

Sources: [data.binance.vision](https://github.com/binance/binance-public-data#how-to-download-programatically). You can script `curl`/`wget` directly from Python.


## 6) Detailed Roadmap (From "How do I" to "Done")

Do this in vertical slices. If at any point you can't explain a file to yourself, **don't move to the next slice**.

### Phase 0: Foundation (Day 1 - 2) | Goal: Repo + Plan locked in

> **Status: ✅ COMPLETE (2026-11-01)** — first commit `d9b98ee`, pushed to https://github.com/ZeroiJ/tickforge (public). Env verified: rustc 1.99.1 · python 3.15.2 · R 4.6.2 (+Rscript) · uv 0.13.6. ADR-0001 accepted. Notion/Composio brain not yet linked (`/mcps`).

| Task | Action | Done when |
|---|---|---|
| **Init repo** | `git init`, README skeleton, `.gitignore`, folder structure above. | `git log` has first commit. |
| **ADR setup** | Write `docs/ADR-0001-custom-mkt-format.md`. Explain *why* not just Parquet (you want to learn indexing + pruning). | Can justify in 2-3 lines. |
| **Env setup** | Install Rust (stable), Python 3.11+, R + RStudio/Positron. Use `uv`/`venv` for Python. | `rustc --version`, `python --version`, `R --version` all work. |
| **Agent context** | Add a short `AGENTS.md` to `tickforge/` with goals, constraints, "read diffs", "test small first". | Agent stays on track across sessions. |

### Phase 1: Ingest & Normalize (Day 2 - 5) | Goal: Get clean tick data

| Task | Action | Done when |
|---|---|---|
| **Downloader** | `python/tickforge/download.py` to pull 1 day of `aggTrades` for `BTCUSDT` from `data.binance.vision`. | Zip downloaded + extracted to `data/raw/`. |
| **Schema design** | Define canonical schema: `trade_id`, `symbol`, `price` (f64), `qty` (f64), `quote_qty` (f64), `ts` (i64 ms/us), `is_buyer_maker` (bool), `is_best_match` (bool). | Written in `rust/crates/tf-core/src/schema.rs` + `python/tickforge/schema.py` (single source of truth). |
| **Normalizer** | Parse CSV, coerce types, validate, write to `data/norm/btcusdt/2026-xx-xx/trades.parquet` with `polars/pyarrow`. | `duckdb` can query it and counts match raw rows. |
| **Data contract tests** | Basic Python checks: no nulls on keys, `ts` monotonic within file, positive price/qty. | `pytest` passes on that day. |

### Phase 2: Custom Columnar Format `.mkt` (Week 1 - 2) | Goal: Your engine's heart

This is what'll separate you from just "data analysis" repos.

| Task | Action | Done when |
|---|---|---|
| **Spec** | Write `docs/format-spec.md`. Cover magic bytes (`TFMK`), version, footer, column sections, row groups/chunks, min/max per chunk, compression (`zstd`). | Clear enough for future-you to implement. |
| **Writer (`tf-format`)** | In Rust: write columns separately (timestamp, price, qty, side/flags). Chunk by ~10^6 rows or fixed byte target. | `tf-cli ingest-norm --in data/norm/... --out data/mkt/` produces `.mkt`. |
| **Reader + memmap** | Use `memmap2` to read footer/index first. Don't load whole file. | Can read just `ts` column range without deserializing price/qty. |
| **Round-trip test** | Load `.mkt` and compare `ts,price,qty` to the Parquet source byte-for-byte by values. | 100% match on sample day. |
| **Compression check** | Log raw vs `.mkt` size. Aim for 4-8x on trades. | Numbers in README. |

*Tip*: Look at how Vortex does per-column encodings conceptually. You don't need dictionary+RLE+frame-of-reference all at once. **Dictionary + run-length for bools + plain + zstd is more than enough to start.**

### Phase 3: Query Engine + Replay (Week 2 - 3) | Goal: Prove it can handle "big" data

Focus on the exact drills from earlier, but enforced by tests.

| Task | Action | Done when |
|---|---|---|
| **Time-range scan** | `tf-engine` with predicate `ts BETWEEN t1 AND t2`. Use chunk min/max to skip. | Query 10 min window over 30GB returns in < 200ms (local). |
| **Resample** | Build 1s OHLCV from trades (open/high/low/close/volume/trades/count). | Matches DuckDB on same range to the tick. |
| **Deduplication** | Enforce `trade_id` + `(symbol, ts, price, qty)` key. Track seen in a hash set with spill? Or just check by key on ingest. | Drill: insert same 5 events -> count unchanged. |
| **Out-of-order handling** | Allow small skew (e.g. 2-5s) or sort by `ts` on write. Log "late" count. | Late events land in correct 1s bar. |
| **Replay mode** | `tf-cli replay --speed 100x --from <ts> --to <ts>` streams events from disk in time order. | Feels like watching ticks fly. Great for demo. |
| **Bench vs DuckDB** | Time identical query on `.mkt` vs `.parquet`. Record in `docs/benchmarks.md`. | Table with cold/warm, rows scanned/skipped. |

### Phase 4: Microstructure Research in R (Week 3 - 4) | Goal: Actual finance insight

This is what makes it stand out for Data/Analytics roles.

| Task | Action | Done when |
|---|---|---|
| **Export slice** | Use DuckDB/Polars to dump 1-7 days to `r/data/` (or read Parquet directly). Keep < 2GB for R. | R loads fast. |
| **Features** | Compute: mid-price, spread, signed volume (buy/sell), orderflow imbalance, log-returns, realized volatility (5s/10s windows). | Clean dataframe in `data.table`. |
| **Hypothesis test** | Pick **one** question: *"Does signed volume/orderflow imbalance predict next 10s mid-price change?"* Run correlation + simple linear regression + autocorr. | Clear answer: yes/no + effect size. |
| **Visualization** | `ggplot2` plots: time series, scatter with trendline, ACF/PACF, rolling volatility. | Looks clean, readable. |
| **Writeup** | Put findings in `r/REPORT.md` with numbers, charts, and conclusion. Don't just say "it works". | Something you'd actually show in an interview. |

### Phase 5: Lakehouse Ready (Databricks) (Week 4 - 5) | Goal: Touch real-world tooling

You want to be able to say "I can land this in a lakehouse".

| Task | Action | Done when |
|---|---|---|
| **Partitioning** | Reorganize `data/lake/` to `exchange=BINANCE/symbol=BTCUSDT/dt=YYYY-MM-DD/`. Mirror Crypto Lake style. | Matches industry layout. |
| **Delta conversion** | Use `deltalake` (Python) or `pyspark` to write as **Delta Lake** tables (`trades_delta`). | `_delta_log/` exists. |
| **Databricks upload** | Sign into [Databricks Community Edition](https://community.cloud.databricks.com/). Create a Catalog/Schema, upload to **Volumes** (`/Volumes/<catalog>/<schema>/<volume>/tickforge/`). | Visible in Data Explorer. |
| **Query in SQL** | In Databricks SQL: `SELECT date_trunc('hour', from_unixtime(ts/1000)), sum(qty) FROM trades_delta GROUP BY 1`. | Runs without errors. |
| **(Optional) dbt** | If time: `dbt init`, source from Delta/Parquet, create `stg_trades`, `int_bars_1s`, `marts_features`. | `dbt run && dbt test` green. |

### Phase 6: Polish, Proofs & Resume (Week 5 - 6) | Goal: Make it demo-ready

This is the most overlooked part. A messy repo with GBs of data loses to a clean repo with solid proof.

| Task | Action | Done when |
|---|---|---|
| **README overhaul** | Write `README.md` like `p1-market-data-lake`. Include: intro, architecture diagram (can use Mermaid), quickstart, layout, results. | Looks professional on GitHub. |
| **Proof table** | Add a "Proofs" section with the 3+ drills + numbers. Show before/after counts. | Matches the idea you were going for. |
| **ADRs** | Fill at least 2 more: `ADR-0002-dedup-key-choice.md`, `ADR-0003-time-range-pruning.md`. Explain tradeoffs. | Shows design thinking. |
| **Benchmarks** | `docs/benchmarks.md` with query times, file sizes, compression ratio, rows/sec. | Tangible numbers. |
| **Repo hygiene** | Clean commits, meaningful messages, no secrets, data gitignored. Pin to top on GitHub. | Looks sharp. |
| **Resume bullets** | Draft 2-3 bullets (see below). | Ready to paste. |


## 7) The "Must Prove" Drills

Put these as actual commands + results in your README. This is what'll make people believe it.

| Drill | How to test | Expected |
|---|---|---|
| **1) Idempotent ingest** | Run `tf-cli ingest-norm` twice on same files | Second run says `written=0`, no new `.mkt` rows. |
| **2) Kill & resume** | Start writing `.mkt`, kill terminal (`Ctrl+C`/taskkill) mid-chunk, rerun | No corruption. Either continues or detects incomplete footer and rebuilds safely. |
| **3) Deduplication** | Feed 5 identical `(trade_id,symbol,ts,price,qty)` records | Engine reports `inserted=1, dupes=4`. `COUNT(*)` unchanged. |
| **4) Late/out-of-order** | Inject a trade with `ts = now - 90s` into a "live" window | Appears in correct 1s bar, `late_events++` incremented. |
| **5) RAM-safe scan** | Query 24h of 5 symbols from `.mkt` with Task Manager open | RAM stays < 1-2GB, disk I/O visible, answer correct vs DuckDB. |
| **6) Replay integrity** | Replay full day at 1x, aggregate to 1m bars | Byte-for-byte equal to bars built by full pass. |


## 8) First 24 Hours (Action Plan)

Do these in order. Don't jump to "live websocket" until all 6 are done.

| Order | Command/Task | Notes |
|---|---|---|
| **1.** | `mkdir C:\Projects\tickforge` (you already did) + `cd` into it | Session already moved. Good. |
| **2.** | `git init && git branch -M main` | Start clean. |
| **3.** | Create folders from section 4 | Do with `mkdir` or ask agent to scaffold. |
| **4.** | `cargo init --name tf-cli rust/bin/tf-cli` and add workspace in `rust/Cargo.toml` | Get Rust compiling first. |
| **5.** | Download **1 day** `BTCUSDT` `aggTrades` from `data.binance.vision` with Python | Keep it tiny on purpose. |
| **6.** | Write normalizer -> Parquet, verify with `duckdb :memory:` | Proves ingestion works. |
| **7.** | Write minimal `.mkt` header+footer in Rust, read it back | Ugly is fine, working > pretty. |
| **8.** | Commit: `"chore: init tickforge with ingest + mkt skeleton"` | Build momentum. |


## 9) Resume-Worthy Bullets

Once you have the proofs + numbers, use these on LinkedIn/Resume.

- **For Data Engineering roles:** *"Built `tickforge`, a tick-level market data engine in Rust that ingests 100GB+ of Binance trades into a custom memory-mapped columnar format with time-range pruning, deduplication, and replay—querying full-day windows with <2GB RAM."*
- **For Analytics/DS roles:** *"Engineered end-to-end microstructure research on high-frequency crypto data (orderflow, realized volatility) using Rust, Python, and R, testing predictive signals between trade flow and short-horizon returns."*
- **For Lakehouse/Data Platform roles:** *"Landed normalized tick data to Databricks Volumes as Delta Lake, modeled with dbt Core, and validated against industry-style partitioning (`exchange/symbol/dt`)."*


## 10) Advice With Agent Assistance

Since you're using an agent, exploit it smartly.

| Rule | Explanation |
|---|---|
| **Vertical > Horizontal** | Don't say "build the whole engine". Say "write a memmap reader that reads only the footer and returns column offsets". |
| **Read diffs religiously** | Every time the agent writes Rust, open the diff and trace variable flow. If you can't explain `fn read_footer()` without scrolling up, ask it to explain line by line. |
| **Test before scaling** | 1 day must pass all 6 drills before you download 30 days. This will save you so much headache. |
| **Use Databricks early** | Don't wait till end. Upload that 1 day to Databricks Volumes after Phase 5's first step. Gets you comfortable with it now. |
| **Write ADRs as you go** | 10 mins writing an ADR teaches you more than 2 hours of vibe-coding. Forces you to think. |
| **Benchmark honestly** | If your `.mkt` is slower than DuckDB for a query, don't hide it. Figure out why and note it. Honesty looks more mature than fake claims. |
| **Document failures** | Put "Bugs I hit" in README (like you did with `p1`). Shows debugging skill which is gold. |


## My Recommendation For You

**Go all-in on `tickforge` with the Rust custom format.**

- **If you want max DE appeal:** Prioritize Phases 0-3 hard. Benchmarks + drills + custom `.mkt` are your selling point.
- **If you want to lean more "Data Analyst + ML" oriented:** Spend extra time on Phase 4 (R research). A solid, falsifiable hypothesis with clean plots will look incredible for DA roles.
- **If you want to flex cloud/lakehouse:** Do Phase 5 fully. Databricks exposure is very common now and you barely see students doing it on personal projects.

**My pick for starting RIGHT NOW:** Finish Phase 0 and 1 today. Get that first 1-day BTCUSDT parsed and verified. Once you see "rows match" between raw CSV and your normalized Parquet, you'll be locked in.

Start at `C:\Projects\tickforge`. Keep it minimal, prove brutally, scale only when the tiny version is rock solid. That's exactly the kind of project that'll look best on your resume as a student.