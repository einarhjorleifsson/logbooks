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

The primary aim is to merge three fisheries logbook schemas into one unified
format (`trip`, `station`, `catch` parquet files):

| Schema | Convert script | Output | Status |
|---|---|---|---|
| `fs_afladagbok` | `scripts/01_fs_afladagbok_convert.R` | `data/fs_afladagbok/*.parquet` | done |
| `afli` | `scripts/01_afli_convert.R` | `data/afli/*.parquet` | done |
| `adb` | `scripts/01_adb_convert.R` | `data/adb/*.parquet` | done |
| **merged** | `scripts/02_merge.R` | `data/merged/*.parquet` | done |

- `fs_afladagbok` — FisheryScan digital logbooks from Fiskistofa; static dump through 2025-12
- `afli` — legacy Oracle system; primary historical source (~1950–present); 96 tables
- `adb` — live-streaming feed (in-house conversion); uses old gear codes natively
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
- Column renaming via `wk_translate()` using `data/dictionary.parquet` (~141 entries; built by `data-raw/DATASET_dictionary.R`)

---

## Unified output schema (brief)

Full column tables, effort units, and merge details in `AGENTS_output_schema.md`.

Three output tables per schema: `trip.parquet` (one row per voyage),
`station.parquet` (one row per fishing operation), `catch.parquet` (one row per
species per station).

Station columns include: `.tid`, `.sid`, `gid` (new), `gid_old`, `date`,
`t0`/`t1`/`t2` (timing), `lon1`/`lat1`/`lon2`/`lat2`, `sq`/`ssq`, `z1`/`z2`,
`effort_count`, `effort_duration`, `effort_unit`, `effort`, `towtime`,
`gear_width`, `source`.

Effort is two-component: `effort = effort_count × effort_duration`. Units vary
by gear class: `"gear-minutes"`, `"hook-days"`, `"net-days"`, `"jig-hours"`,
`"trap-hours"`, `"setting"`.

**Merged output** uses priority rule afli > fs_afladagbok > adb. Current record
counts (2026-04-12): 7,286,576 stations · 1,838,263 trips · 16,874,343 catch.

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
| `grammar.qmd` | Grammar of sea-going observational data — four-level hierarchy, unified column vocabulary, time-naming convention |
| `afli-tables.qmd` | Full inventory of all 96 afli tables; schema evolution; join paths; wacky coordinate origin |
| `convert-bugs.qmd` | Convert script audit: bugs found and fixed |
| `merge.qmd` | Merge rationale, method, timing quality, catch completeness |
| `wacky_recovery.qmd` | **Combined** wacky coordinate document — discovery, mechanism, recovery, appendices (maths, algorithm, functions) |
| `ramb-list.qmd` | Informal notes |
| `_wackytracks.qmd` | Archived; content absorbed into `wacky_recovery.qmd` |

---

## Known data quality issues

1. **Wacky coordinates** (`sjalfvirkir_maelar`, `rafr_sjalfvirkir_maelar`) — systematic DDMMmm→DMS misconversion by Trackwell/SeaData software; shifts positions ≤ ~0.4 NM; confirmed sender-side; ~4% records unambiguous, ~33% partial, ~63% ambiguous. Full methodology in `wacky_recovery.qmd`.
2. **Erroneous timestamps** — year-1899 and year-2090s entries in `rafr_sjalfvirkir_maelar`; likely Oracle null/default date values.
3. **Convert script bugs** (all fixed 2026-04-11) — `bind_rows` of `inner_join`s silently dropped stations; wrong `date` derivation; missing aux tables; wrong gear code filters; typo in `difftime`; dredge fan-out. Full audit in `convert-bugs.qmd`.
4. **Catch data gap 2023+** — `adb` catch collapses to near-zero from 2023; source issue (not merge artefact); do not use merged catch beyond 2022 without corrected source.
5. **Residual t0 ≤ t1 ≤ t2 violations** — 36–67 in afli (negative-.sid garbage records); 153 in adb static gear. Not corrected; filter on ordering before timing-sensitive analyses.
6. **Unmapped gear codes** — ~4.4% (1,170) of `adb`-only stations have `gid = NA` after old→new gear mapping.

---

## Outstanding Work

- [ ] **Investigate `gid = NA` in `adb`-only stations** — identify which `gid_old` codes are unmapped; decide whether to extend `gear_mapping` or accept the gap.
- [ ] **Apply wacky coordinate recovery at scale** — methodology in `wacky_recovery.qmd` (fwd-bwd smoother; 33–48% turn-angle improvement). Consider contacting Trackwell/SeaData for original DDMMmm integers first (exact recovery). Full archive ~20 M records; parallelise with `furrr::future_map()` or Rcpp. Write corrected parquet to `data/` and notify `../fishydata`.
- [ ] **Fix gid 9 coordinate encoding** in `01_fs_afladagbok_convert.R` — classify Flotvarpa (`ws_veidi`) rows by `uppruni` to separate decimal-degree from DMS sources.
- [ ] **Confirm longline `effort_count` semantics** — is `fj_kroka` the total hook count (`onglar × bjod` aggregated) or number of lines? Treat hook-day values as approximate until confirmed.
- [ ] **Align static-gear time columns with grammar convention** — grammar uses `t0`–`t3`; convert scripts use `t1`/`t2` for static gear. Requires renaming `t1`→`t2`, `t2`→`t3`, adding `t3`; update all downstream effort calculations.

### Completed

- [x] `grammar.qmd` completed (2026-04-11/19) — four-level hierarchy, time-naming convention, fishing sample documentation, cross-schema mapping table
- [x] `merge.qmd` rewritten (2026-04-11) — three-tier rationale and coverage analysis
- [x] `scripts/01_afli_convert.R` audited and fixed (2026-04-11) — inner_join bug, gid-filter guards
- [x] `scripts/01_fs_afladagbok_convert.R` audited and fixed (2026-04-11) — five bugs; output 469,881 stations
- [x] `scripts/01_adb_convert.R` audited and fixed (2026-04-11) — five bugs; output increased to 628,314 stations
- [x] `scripts/02_merge.R` refactored to three-tier (2026-04-12) — 7,286,576 stations, 1,838,263 trips, 16,874,343 catch
- [x] `_targets.R` created (2026-04-12) — 12 targets, 4 tiers, parallelisable Tier-1
- [x] Dictionary script relocated to `data-raw/DATASET_dictionary.R` (2026-04-11)
- [x] `index.qmd` created (2026-04-11)
- [x] `scripts/01_fs_afladagbok_convert.R` restructured (2026-04-12) — shared SCHEMA constant, separate aux blocks, `auxillary.parquet` output
- [x] `data-raw/DATASET_dictionary.R` extended (2026-04-19) — afli gear-detail tables added; ~141 entries total; `adb` block fixed
- [x] `afli-tables.qmd` written and corrected (2026-04-19) — 96-table inventory, three eras, wacky coordinate origin confirmed sender-side
- [x] `scripts/01_afladagb_xml_nmea.R` written (2026-04-19) — extracts NMEA from XML into `data/afli/nmea.parquet`
- [x] `scripts/01_afli_convert.R` select() calls fixed (2026-04-19) — 6 calls updated to use translated names
- [x] `_quarto.yml` navbar reorganised (2026-04-19) — all QMD documents, left-to-right narrative order
- [x] Wacky coordinate documents merged (2026-04-19) — `wackytracks.qmd` + `WACKY_COORDS.md` + `wacky_recovery.qmd` → single `wacky_recovery.qmd`; inverted-pyramid structure; old files archived/deleted
