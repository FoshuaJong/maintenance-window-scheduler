# Asset Scheduler

Constrained scheduling optimiser for network maintenance planning.

## Problem

A planning cycle produces a list of assets requiring maintenance windows. The planner must co-schedule assets with shared dependencies (bundles) into the same window, prohibit concurrent execution of conflicting asset pairs, distribute work into historically preferred months, and balance workload across months and within each season.

Bundling, conflict avoidance, and load-balancing constraints interact — no greedy or rule-based approach satisfies all three simultaneously. CP-SAT (Google OR-Tools) solves the combined problem exactly.

## Approach

Two-stage CP-SAT pipeline:

1. **Month distribution** — assigns each asset to a calendar month
2. **Day scheduling** — assigns a contiguous start date to each group within its season window

Between stages, bundled assets are reduced to their **Minimum Viable Sets (MVS)** — the minimal groups that must be co-scheduled. Stage 2 operates on MVS groups, which reduces variable space and enforces bundling by construction.

---

### Stage 1 — Month distribution

**Sets and indices:**
- `A` — set of assets; `a ∈ A` denotes an individual asset
- `M` — set of calendar months {Jan, …, Dec}; `m ∈ M` denotes an individual month

**Variables:**
- `month_assignment[a, m] ∈ {0, 1}` — asset `a` assigned to month `m`
- `monthly_count[m] ∈ ℤ≥0` — number of assets assigned to month `m`

**Parameters:**
- `preference_penalty[a, m] = 1` if `m` is not in asset `a`'s preferred/historically viable months, else 0
- `avg = ⌊|A| / 12⌋`

**Constraints:**
```
Σ_m month_assignment[a, m] = 1                                          ∀ a ∈ A         (each asset assigned once)
monthly_count[m] = Σ_a month_assignment[a, m]                           ∀ m ∈ M         (concurrency definition)
month_assignment[a, m] = 1 → month_assignment[a', m] = 1               ∀ m, (a,a') ∈ B (bundled assets share a month)
avg − δ ≤ monthly_count[m] ≤ avg + δ                                   ∀ m ∈ M         (load balance)
```
where B is the set of bundle pairs and δ is a free non-negative integer variable.

**Objective:**
```
minimise  Σ_a Σ_m preference_penalty[a, m] · month_assignment[a, m]  +  δ
```
Minimises non-preferred month assignments jointly with workload variance across months.

---

### Stage 2 — Day scheduling

Run independently for each season: Summer (Dec–Feb), Autumn (Mar–May), Winter (Jun–Aug), Spring (Sep–Nov).

**Sets and indices:**
- `A` — set of MVS groups in the season; `a ∈ A` denotes an individual group
- `D` — set of calendar dates in the season window; `d ∈ D` denotes an individual date

**Variables:**
- `start_var[a, d] ∈ {0, 1}` — asset `a` starts on day `d`
- `running_var[a, d] ∈ {0, 1}` — asset `a` is running on day `d`
- `daily_count[d] ∈ ℤ≥0` — number of assets running on day `d`
- `daily_starts[d] ∈ ℤ≥0` — number of assets starting on day `d`

**Parameters:**
- `dur[a]` = maintenance duration in days
- `W = {d ∈ D : d is a weekday, d ∉ public holidays}`
- `avg = ⌊Σ_a dur[a] / |W|⌋`

**Constraints:**
```
Σ_d start_var[a, d] = 1                                                 ∀ a       (exactly one start)
start_var[a, d] = 1 → running_var[a, d+t] = 1  t = 0,…,dur[a]−1      ∀ a, d    (contiguous run)
Σ_d running_var[a, d] = dur[a]                                          ∀ a       (correct total duration)
start_var[a, d] = 0  if d + dur[a] − 1 ∉ D                            ∀ a, d    (no overrun past window)
daily_count[d] = Σ_a running_var[a, d]                                  ∀ d       (concurrency definition)
daily_starts[d] = Σ_a start_var[a, d]                                   ∀ d       (starts definition)
running_var[a1, d] = 1 → running_var[a2, d] = 0                        ∀ d, (a1,a2) ∈ C  (conflict avoidance)
daily_count[d] = 0                                                       ∀ d ∉ W   (no work on non-working days)
avg − δ ≤ daily_count[d] ≤ avg + δ                                     ∀ d ∈ W   (daily load balance)
```
where C is the set of conflicting pairs and δ is a free non-negative integer variable.

**Objective:**
```
minimise  δ  +  max_d daily_starts[d]
```
Minimises daily workload variance jointly with peak simultaneous starts (reduces operational complexity on any single day).

---

## Stack

```
python    >= 3.x
ortools             CP-SAT solver
pandas
numpy
openpyxl
```

## Inputs

| File | Description |
|---|---|
| `data/asset_requests.xlsx` | Assets requested in the current planning cycle |
| `data/historical_durations.xlsx` | Historical median durations and preferred months for all known assets |
| `data/relational_dependencies.xlsx` | Bundle groups and conflict pairs |
| `config.json` | Planning year, file paths, public holidays |

`historical_durations.xlsx` and `relational_dependencies.xlsx` cover the full asset population. `asset_requests.xlsx` is a subset — only assets in the current cycle.

## Outputs

All written to `outputs/` at runtime.

| File | Description |
|---|---|
| `05_season_batches_{season}.xlsx` | Month-level assignment per season |
| `06_mvs_groups_{season}.pkl` | MVS groups per season |
| `07_day_schedule_{season}.xlsx` | Day-level schedule per season |
| `08b_season_schedule_{season}.xlsx` | Season schedule expanded with full bundle membership |
| `09_draft_aop_{year}_to_{year+1}.xlsx` | Merged full-year schedule |

## Limitations

- Duration uses historical median — does not reflect actual work scope for the current cycle
- Public holidays are manually maintained in `config.json`

AI assistance was used to refactor code for readability, generate documentation, and sanitize data for anonymity.
