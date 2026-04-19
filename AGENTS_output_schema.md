# logbooks — Unified output schema and merge details

> Load this file when working on convert scripts, output schemas, the merge
> script, or any analysis that reads `data/merged/` or `data/<schema>/`.

---

## Dictionary

Column renaming from source vocabulary is handled by `wk_translate()` (from the
`whack` package) using `data/dictionary.parquet` (built by
`data-raw/DATASET_dictionary.R`).

| Column | Description |
|---|---|
| `clean` | Unified / target column name |
| `messy` | Raw source column name |
| `schema` | Source system (`"afli"`, `"fs_afladagbok"`, `"adb"`) |

Convert scripts filter to their schema at the top:

```r
SCHEMA <- "fs_afladagbok"
dictionary <- read_parquet("data/dictionary.parquet") |> filter(schema == SCHEMA)
```

No bare `rename()` calls should remain in convert scripts except for
truly table-specific ambiguous names (e.g. `id` appears in multiple
`fs_afladagbok` tables and is renamed inline; `breidd` in `ws_plogur` means
plow width, not latitude — kept as an inline rename to avoid polluting the
schema-level dictionary). Dictionary is currently ~141 entries total.

---

## `trip.parquet`

One row per fishing trip (voyage). `.tid` origin differs by schema:
- `afli`: derived as `min(.sid)` within `(vid, T2, hid2)` — not a raw source column; `T1` is the minimum fishing date within the trip.
- `fs_afladagbok`: taken directly from `ws_veidiferd.id`.

| Column | Type | Description |
|---|---|---|
| `.tid` | int | Trip identifier |
| `vid` | int | Vessel identifier |
| `T1` | datetime | Departure time (or earliest fishing date for `afli`) |
| `hid1` | int | Departure harbour ID (`NA` for `afli`) |
| `T2` | datetime | Landing date/time |
| `hid2` | int | Landing harbour ID |
| `n_crew` | int | Crew count |
| `source` | chr | Source system tag (within `fs_afladagbok` schema; `NA` for `afli`) |
| `schema` | chr | Schema tag (`"afli"`, `"fs_afladagbok"`, `"adb"`) |

---

## `station.parquet`

One row per fishing operation (set/tow/haul). Gear-specific effort columns are
populated only for the relevant gear classes; others are `NA`.

| Column | Type | Description |
|---|---|---|
| `.tid` | int | Trip identifier |
| `.sid` | int | Station identifier |
| `gid` | int | Gear type code — "new" (`fs_afladagbok`) version; unified across schemas |
| `gid_old` | int | Counterpart old-version gear code; `NA` where no mapping exists |
| `date` | date | Fishing date |
| `t0` | datetime | Gear deployment starts (static: first hook/net in water; mobile: warp in water — rarely recorded) |
| `t1` | datetime | Tow start (mobile) / gear retrieval start (static) |
| `t2` | datetime | Tow end (mobile) / gear retrieval end (static) |
| `lon1`, `lat1` | dbl | Start position (decimal degrees) |
| `lon2`, `lat2` | dbl | End position (decimal degrees) |
| `sq` | int | Statistical square [1–999]; out-of-range → `NA` |
| `ssq` | int | Statistical sub-square [0–4]; out-of-range → `NA` |
| `z1`, `z2` | dbl | Start / end depth |
| `effort_count` | dbl | Count component of effort (hooks, nets, jigs, trap units, or simultaneous gear count) |
| `effort_duration` | dbl | Time component of effort in the natural unit implied by `effort_unit` |
| `effort_unit` | chr | Unit label for the `effort_count × effort_duration` product |
| `effort` | dbl | Standardised effort: `effort_count × effort_duration` |
| `towtime` | dbl | Tow duration in minutes (mobile gears) |
| `gear_width` | dbl | Effective gear width (mobile gears) |
| `source` | chr | Schema tag |

> **Time-column convention note.** `t0`–`t2` follow the Icelandic source
> convention. The grammar in `grammar.qmd` uses `t0`–`t3` where `t2` =
> retrieval start and `t3` = retrieval end for all gear types. Aligning the
> convert scripts is listed in Outstanding Work (AGENTS.md).

---

## `catch.parquet`

One row per species per station. Orphan catch records (no matching station)
dropped via `inner_join`.

| Column | Type | Description |
|---|---|---|
| `.sid` | int | Station identifier |
| `sid` | int | Species identifier |
| `catch` | dbl | Total catch (summed within `.sid × sid`) |
| `source` | chr | Schema tag |

---

## `auxillary.parquet` (fs_afladagbok only)

Written by `scripts/01_fs_afladagbok_convert.R`. Combines rows from all five
gear-specific raw tables into one long table keyed on `.sid`, keeping
gear-specific fields out of the main station table.

---

## Effort units by gear class

### `afli` schema (old `gid` codes)

`effort = effort_count × effort_duration`. `effort_duration` derived from
capped `duration_m` (minutes) converted to the natural unit per gear type.

| Class | `gid_old` codes | `effort_count` | `effort_duration` | `effort_unit` |
|---|---|---|---|---|
| Mobile trawl | 6, 7, 8, 9, 14, 15, 38, 40 | `n_units` (`tvo_veidarf`; default 1) | `duration_m` (capped per gear) | `"gear-minutes"` |
| Mobile seine | 5, 26 | 1 | 1 | `"setting"` |
| Longline | 1 | `n_total` hooks (`fj_kroka`, or `onglar × bjod`) | `duration_m / (60 × 24)` | `"hook-days"` |
| Gillnet | 2 | `n_nets` (`dregin`) | `duration_m / (60 × 24)` | `"net-days"` |
| Hand line | 3 | `n_jigs` (`faeri`) | `duration_m / 60` | `"jig-hours"` |
| Traps | 18, 39 | `n_units` (`gildrur`) | `duration_m / 60` | `"trap-hours"` |
| Purse seine / ring net | 10, 12 | 1 | 1 | `"setting"` |

### `fs_afladagbok` schema (new `gid` codes)

Same two-component structure. Not yet fully implemented in
`01_fs_afladagbok_convert.R`.

| Class | `gid` codes | `effort_count` | `effort_duration` | `effort_unit` |
|---|---|---|---|---|
| Mobile trawl | 6, 7, 8, 9, 15 | `n_units` (default 1) | `duration_m` from `difftime(t2, t1)`; capped | `"gear-minutes"` |
| Mobile dragnót | 11 | 1 | 1 | `"setting"` |
| Longline | 12, 13, 21 | `n_hooks` (`fj_kroka`) | `duration_m / (60 × 24)` | `"hook-days"` |
| Gillnet | 2, 3, 4, 5 | `n_nets` (`fj_dreginna_neta`) | `duration_m / (60 × 24)` | `"net-days"` |
| Hand line | 14 | `n_jigs` (`fj_faera`) | `duration_m / 60` | `"jig-hours"` |
| Traps | 16 | `n_units` (`fj_gildra`) | `duration_m / 60` from `difftime(t1, t0)` | `"trap-hours"` |
| Purse seine | 10 | 1 | 1 | `"setting"` |

**Towtime caps for `fs_afladagbok` mobile (hours):** Dragnót (11) → 4;
Botnvarpa (6) → 12; Humarvarpa (7) → 12; Rækjuvarpa (8) → 16; Flotvarpa
(9) → 30; Plógur (15) → 20.

---

## Merged output (`data/merged/`)

Produced by `scripts/02_merge.R`.

### Decision rule (priority: afli > fs_afladagbok > adb)

| Tier | Condition | Source used |
|---|---|---|
| 1 | `(vid, date)` present in `afli` | `afli` |
| 2 | `(vid, date)` not in `afli`, but in `fs_afladagbok` | `fs_afladagbok` |
| 3 | `(vid, date)` not in either | `adb` |

**Rationale:**
- `afli` > `fs_afladagbok`: afli mobile-gear timing derived from on-bottom clock + recorded tow duration → more accurate than electronic timestamps.
- `fs_afladagbok` > `adb`: native "new" `gid` codes and documented coordinate conversion; adb requires lossy old→new gear lookup and has less reliable coordinates.

**Date bounds:**
- `afli`: no lower bound; `date ≤ 2026-12-31`
- `fs_afladagbok`: `2020-01-01 ≤ date ≤ 2025-12-31` (static dump)
- `adb`: `2020-01-01 ≤ date ≤ 2026-12-31` (fills 2026 and fs gaps)

**`gid` for `adb`-only stations:** `adb/station.parquet` stores old gear codes
in `gid_old`. At merge time `gid` is derived by joining `gid_old` against
`gear_mapping` (old→new). ~4.4% (1,170 of 26,361) have `gid = NA` where no
mapping exists.

**`.tid` type:** `afli` and `fs_afladagbok` `.tid` are numeric; `adb` `.tid` is
character. In merged output all stored as character.

### Record counts (2026-04-12)

| Table | afli | fs-only | adb-only | merged |
|---|---|---|---|---|
| station | 6,885,534 | 374,681 | 26,361 | 7,286,576 |
| trip | 1,677,156 | 154,534 | 6,573 | 1,838,263 |
| catch | — | — | — | 16,874,343 |

### Year-by-year station source (2018–2026)

| Year | afli | fs_afladagbok | adb |
|---|---|---|---|
| 2018 | 125,405 | 0 | 0 |
| 2019 | 118,668 | 0 | 0 |
| 2020 | 102,466 | 17 | 364 |
| 2021 | 87,962 | 22,363 | 32 |
| 2022 | 38,075 | 65,727 | 191 |
| 2023 | 1,889 | 95,402 | 0 |
| 2024 | 13 | 97,701 | 1 |
| 2025 | 0 | 93,471 | 5,243 |
| 2026 | 0 | 0 | 20,530 |
