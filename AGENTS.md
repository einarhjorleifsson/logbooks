# logbooks — Agent Memory

> **Preliminary.** This file captures what is currently known. Many sections
> are incomplete and should be updated as the project develops.

## Project overview

Documentation of the structure, quality, and processing of Icelandic fishing
logbook data. Raw data originate from Oracle database dumps stored as parquet
files in `data-raw/data-dump/`.

The primary aim of the **logbooks** project is to merge two different fisheries
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
| `fs_afladagbok` | `scripts/01_fs_afladagbok_convert.R` | `data/fs_afladagbok/*.parquet` | pending |
| `adb` | `scripts/01_adb_convert.R` | `data/adb/*.parquet` | pending |

Two additional data sources exist but have lower priority:

* `data-raw/data-dump/adb` — live-streaming feed from Fiskistofa, converted
  in-house to partially emulate the historical format; effectively the same
  data as `fs_afladagbok` but in a different layout.
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
stored in DDMMss integer format (columns `hsi`/`hei`, `x1`/`y1`, `x2`/`y2`)
and are converted to decimal degrees via `geo::geoconvert.1()` (multiplied by
100 before passing in; longitude negated for West).

| Table (parquet) | Key columns | Description |
|---|---|---|
| `afli.parquet` | `visir`, `tegund`, `magn` | Catch records by species |
| `stofn.parquet` | `visir`, `skipnr`, `veidarf`, `dags`, `hsi`, `hei`, `x1`, `y1`, `x2`, `y2` | Fishing station/event records; `visir` is the unique station key |
| `toga.parquet` | `visir`, … | Tow/mobile gear records |
| `lineha.parquet` | `visir`, … | Longline and gillnet records |
| `gildra.parquet` | `visir`, … | Trap records |
| `hringn.parquet` | `visir`, … | Purse seine / ring net records |
| `rafr_stofn.parquet` | `visir`, `skipnr`, `veidarf` | Header table for automatic logger events; links to `rafr_sjalfvirkir_maelar` |
| `rafr_sjalfvirkir_maelar.parquet` | `visir`, `timi`, `lengd`, `breidd`, `skip_hradi`, `skip_stefna`, `vindhradi`, `vindstefna` | Automatic logger positions (**affected by wacky coordinate error** — see below) |

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

| Column | Description |
|---|---|
| `version` | `"old"` (afli) or `"new"` (fs_afladagbok) |
| `gid` | Gear code in this version |
| `veiðarfæri` | Icelandic gear name |
| `map` | Canonical counterpart gid in the **other** version |
| `gear` | ICES GearType code (e.g. `"OTB"`, `"LLS"`) |
| `target` | ICES TargetAssemblage code (e.g. `"DEF"`, `"SPF"`) |

**`map` semantics** — one-way canonical lookup only. For "new" rows `map`
gives the corresponding old gid; for "old" rows `map` gives the corresponding
new gid. The mapping is many-to-one in both directions because the old schema
used more granular sub-types (e.g. three mesh-size variants of seine net → one
"Dragnót"; five midwater trawl sub-types → one "Flotvarpa"). Round-trips
(old → new → old or new → old → new) are therefore lossy for collapsed gears;
the reverse lookup always returns the canonical representative, not the
original sub-type. Do **not** rely on `map` for bijective conversion.

The "old" side includes codes 91 (Skötuselsnet), 92 (Grálúðunet), and 99
(Óskráð veiðarfæri) which are not recorded in the afli logbooks but exist as
targets of `map` from "new" codes.

### `fs_afladagbok/` — FisheryScan digital logbooks (static dump)

Modern electronic submission format from Fiskistofa; records through 2025-12.
Key tables: `ws_veidiferd.parquet` (trips), `ws_veidi.parquet` (events),
`ws_afli.parquet` (catch).

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
`scripts/01_afli_convert.R` is the reference implementation. The pending
`fs_afladagbok` and `adb` scripts should produce tables that conform to the
same column names and semantics.

Column renaming from source vocabulary is handled by `wk_translate()` using
`data/dictionary.parquet` (built by `scripts/00_DATASET_dictionary.R`).

### `trip.parquet`

One row per fishing trip (voyage). `.tid` is derived as `min(.sid)` within
`(vid, D2, hid2)` — it is not a raw source column.

| Column | Type | Description |
|---|---|---|
| `.tid` | int | Trip identifier (derived) |
| `vid` | int | Vessel identifier |
| `D1` | date | Departure date |
| `hid1` | int | Departure harbour ID |
| `D2` | date | Landing date |
| `hid2` | int | Landing harbour ID |
| `n_crew` | int | Crew count |
| `source` | chr | Schema tag (`"afli"`, `"fs_afladagbok"`, `"adb"`) |

### `station.parquet`

One row per fishing operation (set/tow/haul). Gear-specific effort columns are
populated only for the relevant gear classes; others are `NA`.

| Column | Type | Description |
|---|---|---|
| `.tid` | int | Trip identifier |
| `.sid` | int | Station identifier |
| `gid` | int | Gear type code (unified) |
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

**Gear classes, `gid` codes, and effort units (`afli` system):**

| Class | `gid` codes | `effort_unit` | Notes |
|---|---|---|---|
| Mobile (trawl/seine-trawl) | 6, 7, 8, 9, 14, 15, 38, 40 | `"hours towed"` | `effort = towtime / 60`; towtime capped per gear |
| Mobile (seine/trap-net) | 5, 26 | `"setting"` | `effort = 1`; towtime capped at 4 h |
| Longline | 1 | `"hooks"` | `effort = onglar × bjod`; `onglar` capped at 1800, `bjod` at 100 |
| Gillnet | 2 | `"netnights"` | `effort = dregin × naetur`; `dregin` capped at 200, `naetur` at 7 |
| Hand line | 3 | `"hookhours"` | `effort = faeri × hours` |
| Traps | 18, 39 | `"traphours"` | `effort = n_units × hours`; units and hours capped per gear |
| Purse seine / ring net | 10, 12 | `"setting"` | `effort = 1` |

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

## Known data quality issues

1. **Wacky coordinates** in `rafr_sjalfvirkir_maelar.parquet` — see above.
   Recovery method is documented and implemented; production application pending.
2. **Erroneous timestamps** — year-1899 and year-2090s entries exist in
   `rafr_sjalfvirkir_maelar.parquet`; likely default/null date values from Oracle.
3. Further quality issues TBD as the project develops.

## Open questions / TODO

- Apply coordinate recovery to the full 2008–2020 dataset and write corrected
  parquet; notify `../fishydata` when available.
- Performance: `resolve_track_fb()` inner loop is pure R — consider Rcpp or
  `furrr::future_map()` for the full ~20M record dataset.
- Extend `structure.qmd` to document the full data hierarchy
  (Trip → Station → Catch → Sample).
- Clarify which coordinate columns in the `logbook/` and `fs_afladagbok/`
  systems (if any) are affected by the same wacky conversion.
