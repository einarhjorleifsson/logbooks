# logbooks — Agent Memory

## Sub-files (load on need-basis)

| File | Load when… |
|---|---|
| `AGENTS_data_sources.md` | Working on raw data, convert scripts, or the dictionary |
| `AGENTS_output_schema.md` | Working on convert scripts, output schemas, merge script, or reading `data/merged/` or `data/<schema>/` |

---

## Project overview

Documentation of the structure, quality, and processing of Icelandic fishing
logbook data. Raw data originate from Oracle database dumps stored as parquet
files in `data-raw/data-dump/`.

The primary aim is to merge two fisheries logbook schemas into one unified
format (`trip`, `station`, `fishing_sample`, `catch` parquet files):

| Schema | Convert script | Output | Status |
|---|---|---|---|
| `fs_afladagbok` | `scripts/01_fs_afladagbok_convert.R` | `data/fs_afladagbok/*.parquet` | done |
| `afli` | `scripts/01_afli_convert.R` | `data/afli/*.parquet` | done |
| `adb` | `scripts/01_adb_convert.R` | `data/adb/*.parquet` | not in merge |
| **merged** | `scripts/02_merge.R` | `data/merged/*.parquet` | done |
| **siritar** | `scripts/01_siritar.R` | `data/afli/sensor_GPS.parquet` | in progress |

`scripts/01_siritar.R` — AIS-based wacky coordinate recovery for `sjalfvirkir_maelar`/`rafr_sjalfvirkir_maelar`. Interpolates corrected lon/lat from AIS tracks (`data/trail`) onto GPS logger timestamps; retains original wacky coords as `w_lon`/`w_lat`. `dt_sec` is the time gap between adjacent rows in the combined gps+AIS sequence — an approximation of the interpolation interval (larger → less reliable position). `approx(..., rule = 1)`: lon/lat will be `NA` where AIS does not bracket the GPS timestamp.

- `fs_afladagbok` — FisheryScan digital logbooks from Fiskistofa; static dump through 2025-12
- `afli` — legacy Oracle system; primary historical source (~1950–present); 96 tables
- `adb` — live-streaming feed; **excluded from merge** (old gear codes; less reliable coordinates)
- `logbook/` — experimental restructuring of `fs_afladagbok`; not actively used

Key structural differences across schemas: start/end of fishing activity
recorded differently; numerical gear code systems differ (old vs new).

---

## Relationship to `../fishydata`

| | logbooks | fishydata |
|---|---|---|
| **Role** | QA, documentation, methodological work | Production ETL pipeline |
| **Reads from** | `data-raw/data-dump/` (Oracle parquet dumps) | own `data-raw/` + same Oracle dumps |
| **Outputs** | Quarto HTML site; corrected parquet (future) | `data/logbooks/`, `data/ais/` parquet |

**Practical rules:**
- QA findings → eventually reflected in `../fishydata/scripts/12_DATASET_logbooks*.R`.
- Corrected parquet → write to `data/` and cross-reference in fishydata's AGENTS.md.
- Coding conventions shared with fishydata — see that project's AGENTS.md for canonical version.
- Do **not** duplicate fishydata schema documentation here; link instead.

---

## Data sources (brief)

Full schema documentation in `AGENTS_data_sources.md`.

- `data-raw/data-dump/fs_afladagbok/` — 8 parquet tables; `ws_veidiferd` (trips), `ws_veidi` (stations), `ws_afli` (catch), plus 5 gear-detail tables
- `data-raw/data-dump/afli/` — 96 tables; key ones for convert: `stofn` (6.9 M stations), `afli` (catch), `toga`, `lineha`, `gildra`, `hringn`; also notable: `rafr_sjalfvirkir_maelar` (23.6 M logger positions, **wacky coordinates**)
- `data/gear/gear_mapping.parquet` — maps old ↔ new gear codes; ICES metier vocabulary; built by `data-raw/DATASET_gear-codes.R`
- Column renaming via `wk_translate()` using `data/dictionary.parquet` (~154 entries; built by `data-raw/DATASET_dictionary.R`)

---

## Unified output schema (brief)

Full column tables, effort units, and merge details in `AGENTS_output_schema.md`.

Four output tables per schema:
- `trip.parquet` — one row per voyage (`.tid`, `vid`, `T1`, `hid1`, `T2`, `hid2`, `n_crew`, `source`, `schema`)
- `station.parquet` — narrow spatial/temporal envelope (`.sid`, `.tid`, `date`, `lon1/lat1/lon2/lat2`, `z1/z2`, `schema`)
- `fishing_sample.parquet` — gear and effort detail, 1:1 with station (`.sid`, `.tid`, `gid`, `gid_old`, `gear`, `target2`, `t1`–`t4`, `duration_m`, effort columns, gear dims `g_mesh/g_width/g_length/g_height`, `back_entry`, `schema`)
- `catch.parquet` — one row per species per station (`.sid`, `sid`, `catch`)

Effort is two-component: `effort = effort_count × effort_duration`. Units vary
by gear class: `"gear-minutes"`, `"hook-days"`, `"net-days"`, `"jig-hours"`,
`"trap-hours"`, `"setting"`.

**Merged output** uses two-tier priority: afli > fs_afladagbok. Current record
counts (2026-04-20): 7,260,215 stations · 1,831,690 trips · 16,782,743 catch rows.

---

## Utility functions (`R/`)

| File | Contents |
|---|---|
| `R/translate_name.R` | `translate_name()` — renames columns using a dictionary; works on data frames and lazy duckdb connections |

---

## Pipeline orchestration (`targets`)

Entry point: `_targets.R`. 12 targets across 4 tiers; Tier-1 conversions can
run in parallel with `tar_make(workers = N)`.

| Command | Effect |
|---|---|
| `targets::tar_make()` | Run everything out of date |
| `targets::tar_make("target_name")` | Run one target + dependencies |
| `targets::tar_visnetwork()` | Interactive dependency graph |
| `targets::tar_outdated()` | List targets that would re-run |
| `targets::tar_read("target_name")` | Retrieve completed target value |
| `targets::tar_destroy()` | Wipe cache and start fresh |

---

## Coding conventions

- **dplyr-first**: prefer dplyr pipelines; DuckDB/SQL only for performance.
- **Parquet for storage**: `nanoparquet::write_parquet()` or `arrow::write_parquet()`.
- **duckdbfs for large files**: `duckdbfs::open_dataset()` for files > ~100 MB; `collect()` only what you need.
- **Base R pipe** `|>`, not `%>%`.
- **Comments**: non-obvious reasons only; script head should state input/output files.
- **Section headers**: single-dash style only:
  ```r
  # Header -----------------------------------------------------------------------
  ## Subheader -------------------------------------------------------------------
  ```

---

## Quarto website

- Config: `_quarto.yml` — theme `flatly`, `code-fold: true`, `freeze: auto`
- Site URL: <https://heima.hafro.is/~einarhj/logbooks>
- GitHub: <https://github.com/einarhjorleifsson/logbooks>

**Navbar** (left-to-right): Home · Grammar · Source Data · Pipeline (Convert/Merge) · Coordinates · Notes · GitHub

| File | Topic |
|---|---|
| `index.qmd` | Non-technical landing page |
| `grammar.qmd` | Grammar of sea-going observational data — four-level hierarchy, unified column vocabulary, time-naming convention; sensor layer (two-table structure, station supplement vs time-series); parallel projects pattern (landings, surveys) |
| `afli-tables.qmd` | Full inventory of all 96 afli tables; schema evolution; join paths; wacky coordinate origin |
| `convert-bugs.qmd` | Convert script audit: bugs found and fixed |
| `merge.qmd` | Merge rationale (afli > fs_afladagbok), two-tier method, coverage analysis, timing quality, catch totals |
| `wacky_recovery.qmd` | **Combined** wacky coordinate document — discovery, mechanism, recovery, appendices (maths, algorithm, functions) |
| `afli-backentry.qmd` | Historical back-entry investigation — derivation of `back_entry` flag; 5-step analysis with plots; block summary table |
| `ramb-list.qmd` | Informal notes |
| `_wackytracks.qmd` | Archived; content absorbed into `wacky_recovery.qmd` |

---

## Known data quality issues

1. **Wacky coordinates** (`sjalfvirkir_maelar`, `rafr_sjalfvirkir_maelar`) — systematic DDMMmm→DMS misconversion by Trackwell/SeaData software; shifts positions ≤ ~0.4 NM; confirmed sender-side; ~4% records unambiguous, ~33% partial, ~63% ambiguous. Mathematical recovery documented in `wacky_recovery.qmd`. AIS-based recovery implemented in `scripts/01_siritar.R` (interpolates AIS ground-truth positions; in progress — see bugs listed above).
2. **Erroneous timestamps** — year-1899 and year-2090s entries in `rafr_sjalfvirkir_maelar`; likely Oracle null/default date values.
3. **Historical back-entries** — 95,743 stations (~1.7%) have pre-1980 dates but `.sid > 2.5M`, indicating retrospective digitisation long after the fishing events. Flagged as `back_entry = TRUE` in `fishing_sample.parquet`. Genuine contemporaneous early records (blocks 1–2, `.sid` ≤ 2.34M, 1969–1979) are left as `FALSE`. No random date-entry typos found at scale; all anomalous date clusters are organised multi-vessel batches with catch data. See `afli-backentry.qmd` for full derivation.
4. **Residual t1 ≤ t4 violations** — small number in `afli` (negative-.sid garbage records). Filter `.sid > 0` before timing-sensitive analyses.
5. **DRB duration suspiciously short** — `fs_afladagbok` DRB median ~18 min; may reflect how `upphaf_timi`/`lok_timi` are populated for plow gear. Not investigated.
6. **~54k OTB stations (2021–2022) with no `ws_dragnot_varpa` record** — `effort_unit = NA` for those rows; source completeness gap in `fs_afladagbok`.
7. **`eytt_deleted` records (327 stations)** — 16,912 `rafr_stofn` records have `eytt = 1` (officially deleted in Oracle); 327 passed through into `data/afli/station.parquet` and `fishing_sample.parquet`. Flagged as `eytt_deleted = TRUE` in both tables. `rafr_*` era records have `eytt_deleted = FALSE`; paper-era and Phase 4 direct-load records have `eytt_deleted = NA`. See `afli-tables.qmd` #sec-artefacts.
8. **Two negative-visir pathways in `stofn`** — electronic records with negative `visir` come from two sources: (a) `rafr_*` staging pipeline (~1.1M records, 2003–2020); (b) direct-load bypassing `rafr_*` (~165K records, 2020–2022). Both use the same DDMM coordinate encoding and old gear codes. The `rafr_*` tables are **not** richer in fishing-data terms than `stofn`; they only add XML provenance and audit columns. See `afli-tables.qmd` #sec-master for full characterisation.

---

## Outstanding Work

- [ ] **Finalise `scripts/01_siritar.R`** — schema bug fixed; output renamed to `sensor_GPS.parquet`. Remaining: run at full scale and write corrected parquet to `data/afli/`; notify `../fishydata`.
- [ ] **Characterise AIS coverage gaps** — `approx(..., rule = 1)` produces NA lon/lat where AIS does not bracket GPS timestamps; quantify how many sensor records end up with NA positions.
- [ ] **Fix gid 9 coordinate encoding** in `01_fs_afladagbok_convert.R` — classify Flotvarpa (`ws_veidi`) rows by `uppruni` to separate decimal-degree from DMS sources.
- [ ] **Confirm longline `effort_count` semantics** — is `fj_kroka` the total hook count (`onglar × bjod` aggregated) or number of lines? Treat hook-day values as approximate until confirmed.
- [x] **t0–t3 → t1–t4 reset completed (2026-04-22)** — all convert scripts, dictionary, grammar.qmd, merge.qmd, AGENTS_output_schema.md updated. `t2` (deployment end) is `NA` throughout; `t4` (retrieval end) is `NA` for mobile gear.
- [ ] **Fix DRB `effort_count` in `01_afli_convert.R`** — `n_units` for DRB in `afli` is `NA` through 2011 and `0` from 2012 onward (never a meaningful count); `coalesce(as.integer(n_units), 1L)` currently produces `effort_count = 0` for the post-2011 rows. Hardcode to `1L` as for OTB/OTM.


### Completed

- [x] **DRB `gid_old = 38` spike investigated (2026-04-22)** — confirmed genuine contemporaneous fishery, not back-entry. `gid_old = 38` = Kúffisksplógur (roundnose grenadier plow). 10 vessels, ~16,600 stations across 2017–2018, ~8,100 tonnes of species 199 (grenadier). `.sid` values fully interleaved with coeval OTB records; all via rafr_ pipeline. Spike reflects a 2-year permit window for this deep-water plow fishery. Records are clean; no pipeline action required.
- [x] `rafr_` vs `stofn` strategic review (2026-04-22) — confirmed master tables are authoritative; `rafr_*` adds only XML provenance; two negative-visir pathways documented (rafr_ 2003–2020, direct-load 2020–2022); `eytt_deleted` flag added to `station` and `fishing_sample` outputs; `afli-tables.qmd` updated with Phase 4 and hand-editing sections
- [x] `grammar.qmd` updated (2026-04-21) — sensor layer section added (station supplement vs time-series distinction; multi-table, multi-resolution pattern; NMEA foreshadowing); parallel-projects extension added (landings, surveys, continuous underway); landings section reframed as parallel-project prototype
- [x] `scripts/01_afli_convert.R` audited, fixed, and rewritten (2026-04-11/20) — produces `trip`, `station`, `fishing_sample`, `sensor`, `catch`; gear dims renamed to `g_*` prefix
- [x] `scripts/01_fs_afladagbok_convert.R` rewritten (2026-04-20) — produces `trip`, `station`, `fishing_sample`, `catch`; full effort calc; `auxillary.parquet` dropped; `medal_lengd_neta` added to dictionary; duration = `t4−t1` for all time-based gears
- [x] `scripts/02_merge.R` refactored to two-tier (2026-04-20) — adb dropped; `fishing_sample` added to merge; 7,260,215 stations, 1,831,690 trips, 16,782,743 catch rows
- [x] `merge.qmd` rewritten (2026-04-20) — two-tier afli + fs_afladagbok; timing analysis in fishing_sample; coverage, timing quality, catch sections
- [x] `_targets.R` created (2026-04-12) — 12 targets, 4 tiers, parallelisable Tier-1
- [x] `data-raw/DATASET_dictionary.R` extended (2026-04-19/20) — afli gear-detail tables + `medal_lengd_neta`; 154 entries total
- [x] `afli-tables.qmd` written (2026-04-19) — 96-table inventory, three eras, wacky coordinate origin confirmed sender-side
- [x] `scripts/01_afladagb_xml_nmea.R` written (2026-04-19) — extracts NMEA from XML into `data/afli/nmea.parquet`
- [x] `_quarto.yml` navbar reorganised (2026-04-19) — all QMD documents, left-to-right narrative order
- [x] Wacky coordinate documents merged (2026-04-19) — single `wacky_recovery.qmd`; inverted-pyramid structure
