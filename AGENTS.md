# logbooks — Agent Memory

> **Preliminary.** This file captures what is currently known. Many sections
> are incomplete and should be updated as the project develops.

## Project overview

Documentation of the structure, quality, and processing of Icelandic fishing
logbook data. Raw data originate from Oracle database dumps stored as parquet
files in `data-raw/data-dump/`.

The primary aim of the **logbooks** project is to merge in two different fisheries
logbook schemas into one unified format:

* `data-raw/data-dump/afli` — legacy logbook database (primary historical source)
* `data-raw/data-dump/fs_afladagbok` — the system that superseded the legacy
  database; this is a static dump from the data-provider (Fiskistofa), with
  records through 2025-12; the structure reflects the provider's exact schema

The two datasets partially overlap in time. The main structural differences,
beyond variable naming, are:

* start and end of fishing activity are recorded differently
* numerical gear code systems differ

**Initial phase approach** — produce three comparable output tables (`trip`,
`station`, `catch`) for each schema, following the unified format described in
[§ Unified output schema](#unified-output-schema):

| Schema | Script | Output | Status |
|---|---|---|---|
| `afli` | `scripts/01_afli_convert.R` | `data/afli/*.parquet` | done |
| `fs_afladagbok` | `scripts/01_fs_afladagbok_convert.R` | `data/fs_afladagbok/*.parquet` | done |
| `adb` | `scripts/01_adb_convert.R` | `data/adb/*.parquet` | done |
| **merged** | `scripts/02_merge.R` | `data/merged/*.parquet` | done |

Two additional data sources exist but have lower priority:

* `data-raw/data-dump/adb` — live-streaming feed from Fiskistofa, converted
  in-house to partially emulate the historical format; effectively the same
  data as `fs_afladagbok` but in a different layout. **Uses the old (afli-era)
  gear code system** — gear codes are stored in `gid_old` in the converted
  output; `gid` is absent and must be derived via `gear_mapping` at merge time.
* `data-raw/data-dump/logbook` — an experimental restructuring of
  `fs_afladagbok`; not actively used and no conversion script planned.

The project focuses on:

- Documenting the raw data schemas from multiple source systems
- Identifying and resolving data quality issues
- Producing corrected or annotated datasets that downstream projects can consume

## Relationship to `../fishydata`

A sibling project at `../fishydata` runs the production pipeline that couples
logbook records with AIS vessel-tracking data to produce spatially-enriched
fishing trail datasets.

| | logbooks | fishydata |
|---|---|---|
| **Role** | QA, documentation, methodological work | Production ETL pipeline |
| **Reads from** | `data-raw/data-dump/` (Oracle parquet dumps) | own `data-raw/` + same Oracle dumps |
| **Outputs** | Quarto HTML site; corrected parquet (future) | `data/logbooks/`, `data/ais/` parquet |
| **Audience** | Analysts, documentation readers | Downstream analyses, datacalls |

**Practical rules:**
- New QA findings or correction methods developed here should eventually be
  reflected in `../fishydata/scripts/12_DATASET_logbooks*.R`.
- If a fix produces a corrected parquet file it should be written to
  `data/` in this project and cross-referenced in fishydata's AGENTS.md.
- Coding conventions (dplyr-first, duckdbfs, parquet storage) are shared
  between the two projects — see fishydata's `AGENTS.md` for the canonical
  version.
- Do **not** duplicate fishydata schema or script documentation here; link
  instead.

## Data sources

Raw data live in `data-raw/data-dump/` and are **gitignored**. They were dumped
from an Oracle data server using `data-raw/data-dump/oracle_dump.R`.

### `afli/` — legacy Oracle logbook system (primary)

Converted by `scripts/01_afli_convert.R`. Coordinates in `stofn.parquet` are
stored in DDMM integer format (columns `x1`/`y1`, `x2`/`y2`)
and are converted to decimal degrees via `geo::geoconvert.1()` (multiplied by
100 before passing in; longitude negated for West).

| Table (parquet)  |  Description |
|---|---|---|
| `afli.parquet`   | Catch records by species |
| `stofn.parquet`  | Fishing station/event records; `visir` is the unique station key |
| `toga.parquet`   | Tow/mobile gear records |
| `lineha.parquet` | Longline and gillnet records |
| `gildra.parquet` | Trap records |
| `hringn.parquet` | Purse seine / ring net records |
| `rafr_stofn.parquet` | Header table for automatic logger events; links to `rafr_sjalfvirkir_maelar` |
| `rafr_sjalfvirkir_maelar.parquet` | Automatic logger positions (**affected by wacky coordinate error** — see below) |

**Raw column vocabulary (afli system → unified name):**

| Raw column | Unified name | Meaning |
|---|---|---|
| `visir` | `.sid` | Station identifier |
| `skipnr` | `vid` | Vessel identifier |
| `veidarf` | `gid` | Gear type code |
| `dags` | `date` | Fishing date |
| `tegund` | `sid` | Species code |
| `magn` | `catch` | Catch amount |
| `timi` | `timi` | Timestamp (logger positions) |
| `lengd` | `lengd` | Longitude — logger positions (wacky error) |
| `breidd` | `breidd` | Latitude — logger positions (wacky error) |
| `skip_hradi` | `skip_hradi` | Instantaneous vessel speed (knots) |
| `skip_stefna` | `skip_stefna` | Instantaneous vessel heading (degrees) |

### `gear/` — gear code vocabulary (hand-maintained)

Script: `data-raw/DATASET_gear-codes.R`
Output: `data/gear/gear_mapping.parquet`
Reference: `data-raw/data-dump/gear/asfis.parquet` (loaded but not yet used)

The `gear_mapping` table maps gear codes between the two logbook schemas and
assigns ICES metier vocabulary (`gear`, `target`). One row per gear per version.

**Downstream usage:** the "new" (`fs_afladagbok`) gear codes are the primary
`gid` used in all unified output tables. Station tables carry both:
- `gid` — new-version code (primary, unified across schemas); for `adb`-only
  stations this is looked up during merge via `gear_mapping` (old→new); ~4.4%
  of adb-only stations have `gid = NA` where no mapping exists
- `gid_old` — old-version code; populated for `afli` (native), `fs_afladagbok`
  (via `gear_mapping$map`), and `adb` (native — `adb` uses the old gear code
  system; the column is renamed from `gid` to `gid_old` in `01_adb_convert.R`);

| Column | Description |
|---|---|
| `version` | `"old"` (afli) or `"new"` (fs_afladagbok) |
| `gid` | Gear code in this version |
| `veiðarfæri` | Icelandic gear name |
| `map` | Canonical counterpart gid in the **other** version |
| `gear` | ICES GearType code (e.g. `"OTB"`, `"LLS"`) |
| `target` | ICES TargetAssemblage code (e.g. `"DEF"`, `"SPF"`) |
| `target2` | Custom target code aligned with Icelandic fisheries (partially based on FAO ASFIS; not ICES-validated) |

**`map` semantics** — one-way canonical lookup only. For "new" rows `map`
gives the corresponding old gid; for "old" rows `map` gives the corresponding
new gid. The mapping is many-to-one in both directions because the old schema
used more granular sub-types (e.g. three mesh-size variants of seine net → one
"Dragnót"; five midwater trawl sub-types → one "Flotvarpa"). Round-trips
(old → new → old or new → old → new) are therefore lossy for collapsed gears;
the reverse lookup always returns the canonical representative, not the
original sub-type. Do **not** rely on `map` for bijective conversion.

The "old" side includes codes 91 (Skötuselsnet), 92 (Grálúðunet), and 99
(Óskráð veiðarfæri) as explicit rows in `gear_mapping`, each with a `map`
pointing back to the corresponding new code (1, 22, and 17 respectively).
These codes do not appear in the actual `afli` logbook data (`stofn.parquet`)
but are present in the mapping so that new→old lookups have a valid target.

### `fs_afladagbok/` — FisheryScan digital logbooks (static dump)

Modern electronic submission format from Fiskistofa; records through 2025-12.
Converted by `scripts/01_fs_afladagbok_convert.R`.

| Table (parquet) | Key columns | Description |
|---|---|---|
| `ws_veidiferd.parquet` | `id` (→ `.tid`), `skipnr`, `upphafstimi`, `londunardagur` | Trip records; `id` is the primary key, renamed to `.tid` |
| `ws_veidi.parquet` | `id` (→ `.sid`), `veidiferd_id` (→ `.tid`), `veidarfaeri_id` | Fishing event records; `id` primary key, `veidiferd_id` links to trip |
| `ws_afli.parquet` | `veidi_id` (→ `.sid`), `tegund_id` (→ `sid`), `afli` (→ `catch`) | Catch by species |
| `ws_dragnot_varpa.parquet` | `veidi_id` (→ `.sid`), `grandarar_lengd` (→ `sweeps`) | Mobile gear (trawl, dragnót) — gear width |
| `ws_plogur.parquet` | `veidi_id` (→ `.sid`), `breidd` (→ `gear_width`) | Dredge (plógur) — plow width |
| `ws_linanethandf.parquet` | `veidi_id` (→ `.sid`), `fj_kroka` (→ `n_hooks`), `fj_dreginna_neta` (→ `n_nets`), `fj_faera` (→ `n_jigs`) | Static gear (longline, gillnet, handline) |
| `ws_gildra.parquet` | `veidi_id` (→ `.sid`), `fj_gildra` (→ `n_units`) | Trap records |
| `ws_hringn.parquet` | `veidi_id` (→ `.sid`) | Purse seine / ring net records |

Coordinates in `ws_veidi.parquet` are stored as integers in either DMS
(`DD*10000 + MM*100 + SS`) or DDM (`DD*10000 + MM.CC*100`) format depending on
the data source (`uppruni`). The convert script classifies each trip as `"dms"`
or `"ddm"` using a row-level signal (last-2-digits ≥ 60 is impossible in DMS)
lifted to trip level, combined with a hard list of known DDM sources
(`DDM_SOURCES`). Conversion uses `wk_convert_dms()` / `wk_convert_ddm()` from
the `whack` package.

### `adb/` — live-streaming feed (in-house conversion)

Current live feed from Fiskistofa, converted in-house to partially emulate the
historical `afli` format. The data content is effectively the same as
`fs_afladagbok`, just restructured differently.

### `logbook/` — experimental restructuring (not in use)

An experimental re-layout of `fs_afladagbok`. Not actively developed; no
conversion script planned.

| Table (parquet) | Description |
|---|---|
| `station.parquet` | Fishing stations/events |
| `catch.parquet` | Catch by species |
| `trawl.parquet` | Trawl-specific fields |
| `hook_line.parquet` | Hook-and-line-specific fields |
| `seine_net.parquet` | Seine net records |
| `trip.parquet` | Trip/voyage metadata |

## Unified output schema

Each convert script produces three parquet files written to `data/<schema>/`.
`scripts/01_afli_convert.R` and `scripts/01_fs_afladagbok_convert.R` are the
two reference implementations. The `adb` script should produce tables conforming
to the same column names and semantics.

Column renaming from source vocabulary is handled by `wk_translate()` using
`data/dictionary.parquet` (built by `data-raw/DATASET_dictionary.R`).
The dictionary covers all columns used by both convert scripts; no bare
`rename()` calls should remain in the convert scripts except for truly
table-specific ambiguous names (e.g. `breidd` in `ws_plogur` means plow width,
not latitude — kept as an inline rename to avoid polluting the schema-level
dictionary).

### `trip.parquet`

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

### `station.parquet`

One row per fishing operation (set/tow/haul). Gear-specific effort columns are
populated only for the relevant gear classes; others are `NA`.

| Column | Type | Description |
|---|---|---|
| `.tid` | int | Trip identifier |
| `.sid` | int | Station identifier |
| `gid` | int | Gear type code — "new" (`fs_afladagbok`) version; unified across schemas |
| `gid_old` | int | Counterpart old-version gear code; populated for both `afli` (native) and `fs_afladagbok` (via `gear_mapping$map`); `NA` where no mapping exists |
| `date` | date | Fishing date |
| `t0` | datetime | Gear deployment time (static gears) |
| `t1` | datetime | Start of tow / gear set time |
| `t2` | datetime | End of tow / gear haul time |
| `lon1`, `lat1` | dbl | Start position (decimal degrees) |
| `lon2`, `lat2` | dbl | End position (decimal degrees) |
| `sq` | int | Statistical square [1–999]; out-of-range set to `NA` |
| `ssq` | int | Statistical sub-square [0–4]; out-of-range set to `NA` |
| `z1`, `z2` | dbl | Start / end depth |
| `effort` | dbl | Standardised effort value |
| `effort_unit` | chr | Effort unit (see table below) |
| `towtime` | dbl | Tow duration in minutes (mobile gears; `t1`/`t2` may be unavailable) |
| `gear_width` | dbl | Effective gear width — `sweeps` if available, else `plow_width` (mobile) |
| `source` | chr | Schema tag |

**Gear classes and effort units — `afli` schema (old `gid` codes):**

| Class | `gid_old` codes | `effort_unit` | Notes |
|---|---|---|---|
| Mobile trawl | 6, 7, 8, 9, 14, 15, 38, 40 | `"hours towed"` | `effort = towtime / 60`; towtime derived from `on.bottom` clock field; capped per gear |
| Mobile seine/trap-net | 5, 26 | `"setting"` | `effort = 1`; towtime capped at 4 h |
| Longline | 1 | `"hooks"` | `effort = onglar × bjod`; `onglar` capped at 1800, `bjod` at 100 |
| Gillnet | 2 | `"netnights"` | `effort = dregin × naetur`; `dregin` capped at 200, `naetur` at 7 |
| Hand line | 3 | `"hookhours"` | `effort = faeri × hours` |
| Traps | 18, 39 | `"traphours"` | `effort = n_units × hours`; units and hours capped per gear |
| Purse seine / ring net | 10, 12 | `"setting"` | `effort = 1` |

**Gear classes and effort units — `fs_afladagbok` schema (new `gid` codes):**

| Class | `gid` codes | `effort_unit` | Notes |
|---|---|---|---|
| Mobile trawl | 6, 7, 8, 9, 15 | `"hours towed"` | `effort = towtime / 60`; `towtime = difftime(t2, t1, units = "mins")`; capped per gear |
| Mobile dragnót | 11 | `"setting"` | `effort = 1`; towtime capped at 4 h |
| Longline | 12, 13, 21 | `"hooks"` | `effort = n_hooks × nights`; ⚠ see open question below |
| Gillnet | 2, 3, 4, 5 | `"netnights"` | `effort = n_nets` |
| Hand line | 14 | `"hookhours"` | `effort = n_jigs` |
| Traps | 16 | `"traphours"` | `effort = n_units × hours`; hours from `difftime(t1, t0)` |
| Purse seine | 10 | `"setting"` | `effort = 1` |

Towtime caps for `fs_afladagbok` mobile (in hours): Dragnót (gid 11) → 4 h;
Botnvarpa (6) → 12 h; Humarvarpa (7) → 12 h; Rækjuvarpa (8) → 16 h;
Flotvarpa (9) → 30 h; Plógur (15) → 20 h.

### `catch.parquet`

One row per species per station. Only stations present in `station.parquet`
are retained (orphan catch records are dropped via `inner_join`).

| Column | Type | Description |
|---|---|---|
| `.sid` | int | Station identifier |
| `sid` | int | Species identifier |
| `catch` | dbl | Total catch (summed within `.sid × sid`) |
| `source` | chr | Schema tag |

## Coordinate conversion — the wacky error

A systematic encoding error affects `lengd` and `breidd` in
`rafr_sjalfvirkir_maelar.parquet` (and potentially other tables that passed
through the same conversion). Positions were stored as integers in **DDMMmm**
format (degrees–minutes–decimal-minutes, last two digits = 1/100th of a minute,
range 00–99) but were incorrectly converted to decimal degrees by treating the
last two digits as *seconds* (valid range 00–59). This misplaces reported
positions by up to ~0.4 NM when the decimal-minute value was ≥ 60.

Note: coordinate recovery is a side-product of the main merge effort but is
documented here because it is a significant quality issue.

### Confidence classification

Every affected record can be assigned one of three recovery levels:

| Level | Criterion | Positional uncertainty | Typical share |
|---|---|---|---|
| **High** | Extracted `ss` ∈ [40, 59] for **both** coords | Exact | ~4% |
| **Partial** | Extracted `ss` ∈ [40, 59] for **one** coord | ~300–740 m in one direction | ~33% |
| **Low** | Extracted `ss` ∈ [0, 39]  for **both** coords | ~300–740 m in both directions | ~63% |

### Recovery functions

All utility functions are defined in `wacky_recovery.qmd` (Functions section).
Key entry points:

```r
# Add recovery columns and confidence classification
dat <- add_recovery_cols(dat)   # adds ss_lon, ss_lat, confidence, lon_A/B, lat_A/B

# Resolve ambiguous records (greedy forward pass)
dat |>
  arrange(visir, timi) |>
  group_by(visir) |>
  group_modify(~ resolve_track(.x)) |>    # adds lon_r, lat_r
  ungroup()

# Resolve with forward-backward smoother (preferred for long tracks)
dat |>
  arrange(visir, timi) |>
  group_by(visir) |>
  group_modify(~ resolve_track_fb(.x)) |>  # adds lon_fb, lat_fb
  ungroup()
```

The smoother uses `skip_hradi` and `skip_stefna` (speed and heading) to
dead-reckon the expected next position and select between candidate A and B.
Median turn-angle improvement: **33–48%** by gear type (independent validation).

### DuckDB equivalent

A SQL macro for the *correct* conversion is registered in
`scripts/01_logbooks-old_convert.R`:

```sql
CREATE OR REPLACE MACRO rb_convert_DMdM(x) AS (
  SIGN(x) * (ABS(x) + (200.0/3.0) *
    ((ABS(x)/100.0) - TRUNC(ABS(x)/10000.0) * 100.0)) / 10000.0
);
```

## Utility functions (`R/`)

| File | Contents |
|---|---|
| `R/translate_name.R` | `translate_name()` — renames columns using a dictionary; works on both data frames and lazy duckdb connections |

## Coding conventions

Shared with `../fishydata` — follow those conventions. Summary:

- **dplyr-first**: prefer dplyr pipelines; use DuckDB/SQL only when needed
  for performance or unavailable dplyr equivalent.
- **Parquet for storage**: write outputs with `nanoparquet::write_parquet()`
  or `arrow::write_parquet()`.
- **duckdbfs for large files**: use `duckdbfs::open_dataset()` to query
  parquet files larger than ~100 MB; `collect()` only what you need.
- **Base R pipe** `|>`, not `%>%`.
- **Comments**: non-obvious reasons only; head of each script should state
  input file(s) and output file(s).
- **Section headers in scripts**: single-dash style only:
  ```r
  # Header -----------------------------------------------------------------------
  ## Subheader -------------------------------------------------------------------
  ```

## Quarto website

- Config: `_quarto.yml` — theme `flatly`, `code-fold: true`, `freeze: auto`
- Site URL: <https://heima.hafro.is/~einarhj/logbooks>
- GitHub: <https://github.com/einarhjorleifsson/logbooks>
- Key documents:

| File | Status | Topic |
|---|---|---|
| `wacky_recovery.qmd` | Active | Full coordinate-recovery methodology and worked example |
| `wackytracks.qmd` | Background | Initial exploration of the wacky coordinate patterns |
| `structure.qmd` | Stub | Data structure overview (to be developed) |
| `convert-bugs.qmd` | Active | Convert script audit: inner_join/date bugs and fixes |
| `merge.qmd` | Active | Merge rationale, method, timing quality, and catch completeness assessment |

## Merged output (`data/merged/`)

Produced by `scripts/02_merge.R`.

### Current implementation (three-tier: afli > fs_afladagbok > adb)

Combines all three schemas into a single set of `trip`, `station`, `catch`
parquet files using a priority-ordered decision rule.

**Decision rule (priority order):**

| Tier | Condition | Source used |
|---|---|---|
| 1 | `(vid, date)` present in `afli` | `afli` |
| 2 | `(vid, date)` not in `afli`, but present in `fs_afladagbok` | `fs_afladagbok` |
| 3 | `(vid, date)` not in `afli` or `fs_afladagbok` | `adb` |

**Rationale for tier ordering:**
- `afli` > `fs_afladagbok`: `afli` mobile-gear timing (`t1`, `t2`) is derived
  from the on-bottom clock and recorded tow duration, making it more accurate
  than the electronic timestamps in `fs_afladagbok`.
- `fs_afladagbok` > `adb`: `fs_afladagbok` carries native "new" `gid` codes
  and fully documented coordinate conversion; `adb` uses old gear codes natively
  (requiring a lossy old→new lookup at merge time) and its coordinates are
  considered less reliable.

**Date bounds applied:**
- `afli`: no lower bound; `date ≤ 2026-12-31` (reliable for the full historical period)
- `fs_afladagbok`: `2020-01-01 ≤ date ≤ 2025-12-31` (static dump through 2025-12)
- `adb`: `2020-01-01 ≤ date ≤ 2026-12-31` (fills 2026 and any fs gaps)

**`gid` handling for `adb`-only stations:** `adb/station.parquet` stores old
gear codes in `gid_old` (renamed from `gid` in `01_adb_convert.R`). During
merge, `gid` is derived by joining `gid_old` against `gear_mapping` (old→new).
~4.4% of `adb`-only stations (1,170 of 26,361) have `gid = NA` where no
mapping exists.

**Merged record counts (2026-04-12):**

| Table | afli | fs-only | adb-only | merged |
|---|---|---|---|---|
| station | 6,885,534 | 374,681 | 26,361 | 7,286,576 |
| trip | 1,677,156 | 154,534 | 6,573 | 1,838,263 |
| catch | — | — | — | 16,874,343 |

**Year-by-year station source (2018–2026):**

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

**`.tid` type note:** `afli` and `fs_afladagbok` `.tid` are numeric; `adb`
`.tid` is character. In the merged output all are stored as character.

## Known data quality issues

1. **Wacky coordinates** in `rafr_sjalfvirkir_maelar.parquet` — see above.
   Recovery method is documented and implemented; production application pending.
2. **Erroneous timestamps** — year-1899 and year-2090s entries exist in
   `rafr_sjalfvirkir_maelar.parquet`; likely default/null date values from Oracle.
3. **Convert script station loss** — `01_adb_convert.R`,
   `01_fs_afladagbok_convert.R`, and `01_afli_convert.R` all had bugs that
   silently dropped stations. Fixed 2026-04-11; see `convert-bugs.qmd` for full
   audit. Summary of bugs found and fixed:

   **All three scripts:**
   - **`bind_rows` of `inner_join`s** — stations with no matching gear-detail aux
     record were dropped. Fixed by assembling compact per-gear effort tables and
     `left_join`-ing them onto `base`, retaining all stations (effort = `NA`
     where no aux record exists — genuine upstream data sparsity).

   **`01_adb_convert.R` and `01_fs_afladagbok_convert.R`:**
   - **`date` derived from sparse time column** — both scripts used
     `as_date(t1)` where `t1` mapped to `tow_start` / `milli_timi` (42 % and
     78 % NA respectively). Fixed to `as_date(coalesce(t0, t1, t2))`.

   **`01_adb_convert.R` only:**
   - **Missing `dredge_v` aux table** — `dredge_v.parquet` was never read; it
     covers gid 5, 6, 7, 51, 53 stations absent from `trawl_and_seine_net_v`.
     Added as `dredge_aux` block.
   - **Wrong gear code filters** — `traps` used afli-schema codes 18/39 (adb
     trap data uses gid 51); `static` omitted gid 11, 25, 29, 91, 92.
   - **`difftime(t2, t2, …)` typo** — towtime was always 0; fixed to
     `difftime(t2, t1, …)`.

   **`01_fs_afladagbok_convert.R` only:**
   - **`dredge_aux` fan-out** — stray gid 1/2 rows in `ws_plogur` caused 178
     duplicate `.sid`s across aux blocks, producing fan-out when binding and
     joining. Fixed by adding `filter(gid == 15)` guard to `dredge_aux`.
   - **Longitude sign-flip condition too restrictive** — the guard
     `lon1 >= 10 & lat1 <= 67.75` missed 2,377 gid 6 (bottom trawl) stations,
     leaving coordinates up to 51°E / 85°N. Fixed to `lon1 >= 10 ~ -lon1`
     (Iceland is entirely in the western hemisphere; any positive longitude is
     wrong regardless of latitude).
   - **Known limitation (not fixed):** gid 9 (Flotvarpa / midwater trawl) raw
     coordinates use mixed decimal-degree (1–2 digit integers) and DMS (5–6
     digit integers) encoding in `ws_veidi.parquet`. Cannot be disentangled
     without per-source (`uppruni`) classification. 639 stations retain
     incorrect positive longitudes as a result.
4. **Catch data gap from 2023 onwards**: `adb` catch is near-complete for
   2020–2022 (aligning with afli totals ~1,000 kt/yr) but collapses to
   effectively zero from 2023. The raw `adb/catch.parquet` exhibits the same
   pattern — this is a source issue, not a merge artefact. Merged catch data
   should not be used for catch-based analyses beyond 2022 without a corrected
   source.
5. **Residual t0 ≤ t1 ≤ t2 violations**: small number of ordering violations
   remain in the merged station table (36–67 in afli, all negative-.sid garbage
   records; 153 in adb static gear). Not corrected; filter on ordering before
   timing-sensitive analyses.
6. Further quality issues TBD as the project develops.

## Outstanding Work

- [ ] **Investigate `gid = NA` in `adb`-only stations** — ~4.4% (1,170 of
  26,361) `adb`-only stations have no new-schema `gid` after the old→new
  gear mapping lookup in `02_merge.R`. Identify which old `gid_old` codes
  are unmapped and decide whether to extend `gear_mapping` or accept the gap.
- [ ] **Apply wacky coordinate recovery** to the full
  `rafr_sjalfvirkir_maelar` dataset (2008–2020); write corrected parquet to
  `data/`; notify `../fishydata`.
- [ ] **Fix gid 9 coordinate encoding** in `01_fs_afladagbok_convert.R` —
  classify `ws_veidi` rows for Flotvarpa (gid 9) by `uppruni` to separate
  decimal-degree sources from DMS sources before conversion.
- [ ] **Resolve longline effort unit** — confirm whether `fj_kroka` is total
  hook count or lines×hooks; align `effort_unit` label (`"hooks"` vs
  `"hook-nights"`) and reconcile with the afli formula (`onglar × bjod`).
- [ ] **Clarify `logbook/` coordinate exposure** — determine which coordinate
  columns in the experimental `logbook/` schema (if any) are affected by the
  same wacky conversion as `afli` logger data. The `fs_afladagbok` DMS/DDM
  ambiguity is a separate issue already handled in the convert script.
- [ ] **Performance: `resolve_track_fb()`** — inner loop is pure R; consider
  Rcpp or `furrr::future_map()` for the full ~20M record dataset once recovery
  is applied to production data.

### Completed

- [x] **`structure.qmd` expanded (2026-04-11)** — full grammar-of-fisheries-data
  document: four-level hierarchy (Trip → Station → Sample → Catch), unified
  column vocabulary with naming conventions, and cross-schema mapping table
  for the three Icelandic source systems.
- [x] **`merge.qmd` rewritten (2026-04-11)** — documents the three-tier
  rationale (afli > fs_afladagbok > adb) and the coverage analysis behind
  the fs > adb preference. Note: document still describes two-tier as current
  and three-tier as planned; needs updating to reflect the 2026-04-12 refactor.
- [x] **`scripts/01_afli_convert.R` audited and fixed (2026-04-11)** —
  inner_join bug was present (3,663 stations, 0.05% loss — much smaller than
  adb/fs because afli aux-table coverage is near-complete). Date bug was NOT
  present (date comes from `stofn.vedags`, always populated). Gid-filter guards
  added to each aux block to prevent fan-out on garbage records with negative
  `.sid`.
- [x] **`scripts/01_fs_afladagbok_convert.R` audited and fixed (2026-04-11)**
  — see Known data quality issues item 3 for details. Output: 469,881 stations.
- [x] **`scripts/01_adb_convert.R` audited and fixed (2026-04-11)** — five
  bugs fixed; output increased from ~460k to 628,314 stations.
- [x] **Dictionary script relocated (2026-04-11)** — moved
  `scripts/00_DATASET_dictionary.R` to `data-raw/DATASET_dictionary.R`;
  updated sole downstream reference in `schema-read-check.R`.
- [x] **`index.qmd` created (2026-04-11)** — non-technical landing page for
  the Quarto website.
- [x] **`scripts/02_merge.R` refactored to three-tier (2026-04-12)** —
  implements afli > fs_afladagbok > adb priority; `gid` for `adb`-only stations
  derived via old→new `gear_mapping` lookup at merge time. Output: 7,286,576
  stations, 1,838,263 trips, 16,874,343 catch records.
