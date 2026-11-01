# AGENTS.md | tickforge

> `tickforge` is a tick-level crypto market microstructure lab built in **Rust + Python + R**. The goal is to build a custom, memory-mapped columnar engine (`.mkt`) that can query 100GB+ of Binance tick data with <2GB RAM, prove it with brutal drills, and make it resume-worthy for DE/DA roles.

This file is the source of truth for any AI agent working on this repo. Treat `plan.md` as the execution roadmap and this as the operating doctrine.

## 1) Project Brain: Notion via Composio CLI

We're using **Notion** (via the **Composio CLI/MCP**) as the orchestration brain for this project. Think of it as the "project PM + router" that keeps context tight, delegates work to sub-agents, and enforces the "prove before scaling" rule.

### Setup
- Composio MCP is already installed globally (`composio` server). If not authed yet: run `/mcps` in the OpenCode TUI and sign into `composio`. Make sure the **Notion** connection is linked via Composio.
- Use `composio` CLI/MCP tools where relevant (tool discovery, skills, connections). The Composio tools are available in Code Mode via `tools.composio.*`.
- Notion is to be treated as the long-term memory for this project (tasks, decisions, ADRs index, bugs, learnings). Before starting any non-trivial task, sync context with Notion (see section 3).

### Principles
- **Vertical slices over horizontal scaffolds** - Nation should prefer "write the footer reader" over "scaffold the entire engine".
- **Read diffs** - Never blindly apply large diffs. Nation must summarize diffs and explain risky changes (especially Rust).
- **Prove first** - If we haven't passed the drills for the current slice, do not scale data size.
- **Small context** - Keep only relevant files in context. Offload large logs/benchmarks to files.
- **Justify** - Every architectural decision must be traceable to an ADR in `docs/`.


## 2) Repo Layout

```text
C:\Projects\tickforge\
├── AGENTS.md
├── plan.md
├── README.md
├── docs/        # ADRs, format-spec.md, benchmarks.md
├── rust/        # cargo workspace (tf-core, tf-format, tf-engine, tf-collect, tf-cli)
├── python/      # download/normalize/verify/drills
├── r/           # microstructure research + report
└── data/        # gitignored (raw/, norm/, mkt/, lake/)
```

Rules:
- **Never commit `data/`**. Add to `.gitignore` if not present.
- **Code > binaries**. Show engine code, not GBs of `.mkt`/`.parquet`.
- **Single source of truth for schema**: `rust/crates/tf-core/src/schema.rs` and `python/tickforge/schema.py` must stay in sync.


## 3) Notion Brain Workflow

Use **Notion via Composio** as the project brain and operate in this loop for every task.

### A) Context Sync (Before Work)
1. **Read the plan**: Load `plan.md` and identify the current phase/slice.
2. **Read recent history**: Check last 3-5 git commits (`git log -n 5 --oneline`).
3. **Load project brain**: Use Notion via Composio (search/read relevant pages/databases for `tickforge`) to pull goals, constraints, ADRs index, tasks, bugs, and learnings.
4. **Scan ADRs**: Read any ADRs in `docs/` that relate to the task.
5. **Define acceptance**: Write a short "done when" checklist before writing code.

### B) Delegation (Agent Roles)
Delegate to specialized sub-agents with tight scopes, using Notion as the single source of truth for tasks and decisions.

| Agent | Focus | Allowed Tools |
|---|---|---|
| `rust-engineer` | `rust/` - `.mkt` format, memmap, pruning, engine, CLI | Read/Edit/Write, `shell` (cargo), read diffs |
| `py-glue` | `python/` - download/normalize/verify/drills | Read/Edit/Write, `shell` (uv/venv/pip), run scripts |
| `r-researcher` | `r/` - features, stats, plots, report | Read/Edit/Write, `shell` (Rscript) |
| `data-infra` | Databricks/Delta/partitioning/layout | Read/Edit/Write, `shell` (python with deltalake), Composio tools |
| `qa-verifier` | Drills, benchmarks, correctness vs DuckDB | Read-only heavy + run verification commands |

The orchestrator is responsible for routing, merging diffs, and resolving conflicts between agents.

### C) Execution (Vertical Slice)
1. **Small ask** - Give sub-agent the absolute minimum to implement + test.
2. **Read diff** - Review the full diff before committing.
3. **Verify** - Run the relevant test/drill immediately. Compare against DuckDB when touching data logic.
4. **Explain** - If Rust code is non-obvious, have the agent explain it in plain English.
5. **Document** - If it's an architectural choice, create/update an ADR in `docs/`.

### D) Wrap-up (Persist to Brain)
1. **Summarize** - Write a short summary of what changed, why, gotchas, and next steps.
2. **Update brain** - Create/update the relevant Notion page/database entry (task status, decisions, learnings, bugs) via Composio Notion tools.
3. **Commit** - Make atomic, meaningful commits (e.g. `feat(format): add memmap footer reader`).
4. **Update plan** - Mark checklist items done in `plan.md` if completed.


## 4) Development Rules

### Rust
- **Memmap first**: Use `memmap2`. Never `read_to_end` a multi-GB file.
- **Footer/index first**: Read magic/version/footer before touching column data.
- **Chunking**: Group by ~10^6 rows or target chunk size. Track `min/max` per chunk.
- **Compression**: `zstd` is fine. Keep it simple at first (plain + zstd).
- **Zero-copy mindset**: Avoid unnecessary allocations on hot paths.
- **Errors**: Use `thiserror`. Fail loudly on corruption.
- **Tests**: Unit test round-trip for small fixtures.

### Python
- **Single schema**: Import from shared schema definitions. Don't drift.
- **Idempotent**: Downloads and normalization must be re-runnable.
- **Verify against source**: Always cross-check counts with DuckDB/Polars.
- **Drills as code**: Put kill/restart/dupe/late tests in `python/tickforge/drills.py`.
- **Use `uv` if available**: Faster envs, less friction.

### R
- **Use `data.table`**: For larger slices it's efficient enough.
- **Keep data small**: Export only what you need to `r/data/` (< 2GB).
- **Falsifiable**: Have a hypothesis, test it, report effect size.
- **Reproducible**: Prefer scripts over ad-hoc console work. Note packages used.

### Data & Lakehouse
- **Partition like industry**: `exchange=BINANCE/symbol=<SYMBOL>/dt=YYYY-MM-DD/`
- **Delta when going to lakehouse**: Use `deltalake` for local Delta, upload to Databricks Volumes.
- **Never leak secrets**: Use env vars. No tokens in code/commits.


## 5) Must-Pass Drills

Ensure these pass before moving to the next phase. Run them, log results to `docs/benchmarks.md`, and track their status in Notion via Composio.

| # | Drill | Command (target) | Pass Criteria |
|---|---|---|---|
| 1 | Idempotent ingest | `tf-cli ingest-norm ...` twice | 2nd run: `written=0`, no duplicate rows |
| 2 | Kill & resume | Kill mid-write, rerun | No corruption, completes cleanly or recovers |
| 3 | Deduplication | Inject 5 identical trades | `inserted=1, dupes=4`, count unchanged |
| 4 | Late/out-of-order | Inject trade with `ts = now-90s` | Lands in correct 1s bar, `late_events` incremented |
| 5 | RAM-safe scan | Query 24h with Task Manager open | RAM < 2GB, correct vs DuckDB |
| 6 | Replay integrity | Replay day at 1x -> 1m bars | Matches full-pass bars exactly |


## 6) Composio/Nation Commands (Cheat Sheet)

Use these via Composio CLI/Code Mode as needed.

| Action | How |
|---|---|
| Discover tools | `search({ query: "github" })` or `tools.composio.COMPOSIO_SEARCH_TOOLS` if available via your session |
| Get schemas | `tools.composio.COMPOSIO_GET_TOOL_SCHEMAS({ tool_slugs: [...] })` |
| Load a skill | `tools.composio.COMPOSIO_USE_SKILL({ slug: "..." })` |
| Run remote bash | `tools.composio.COMPOSIO_REMOTE_BASH_TOOL({ command: "..." })` (respect 3min limit) |
| Sync brain | Use Nation via Composio CLI to persist context/learnings after each slice |
| Check connections | `tools.composio.COMPOSIO_WAIT_FOR_CONNECTIONS({ toolkits: [...] })` |


## 7) First Action For Any New Agent Session

When starting fresh in `C:\Projects\tickforge`:

1. `git status && git log -n 3 --oneline`
2. Read `plan.md` -> find current unchecked item in earliest incomplete phase
3. Read `AGENTS.md` (this file)
4. Sync with Nation brain (pull context)
5. Pick **ONE** vertical slice, define "done when", implement, verify, commit
6. Persist learnings to Nation brain


## 8) Success Criteria

`tickforge` is "resume-worthy" when:
- [ ] Custom `.mkt` format with memmap + time-range pruning works
- [ ] All 6 drills pass with logged numbers
- [ ] Benchmarks show RAM-safe querying vs DuckDB
- [ ] R microstructure report has a falsifiable finding with plots
- [ ] Data lands in Databricks Volumes as Delta Lake with industry partitioning
- [ ] `README.md` reads like `p1-market-data-lake` (problem, design, proofs, results)
- [ ] Clean git history, no data committed, solid ADRs
- [ ] Can explain every major design decision without looking at code


**Default stance:** Minimal, provable, explainable. If you can't explain it, don't ship it. Notion keeps us organized and honest to that.