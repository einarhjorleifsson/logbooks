# stasi/logbooks — Agent Memory

## Sub-files (load on need-basis)

| File | Load when… |
|---|---|
| `AGENTS_data_sources.md` | Working on raw data, convert scripts, or the dictionary |
| `AGENTS_output_schema.md` | Working on convert scripts, output schemas, merge script, or reading `data/logbooks/` or `data-raw/logbooks/<schema>/` |

---

## Project overview

QA, documentation, and processing pipeline for Icelandic fisheries data. Covers
four interlocking data domains:

| Domain | Raw source | Key output | Status |
|---|---|---|---|
| **Logbooks** | Oracle dumps (`afli`, `fs_afladagbok`) | `data/logbooks/*.parquet` | done |
| **Landings** | Oracle dump (`aflagrunnur`) | `data-dump/landings/agf/aflagrunnur.parquet` | done (dump only) |
| **Vessel registry** | Oracle dump | `data/vessels/vessels_iceland.parquet` | done (dump only) |
| **AIS** | Oracle STK trail + commercial feeds | `data/trail/` (consolidated, partitioned by year) | done |

The logbook pipeline is the most developed; landings and vessel registry are
currently used as reference tables. AIS data are used both for wacky-coordinate
recovery and for linking vessel tracks to fishing trips.

---

## Logbook pipeline

| Schema | Convert script | Output | Status |
|---|---|---|---|
| `fs_afladagbok` | `data-raw/logbooks/01_fs_afladagbok_convert.R` | `data-raw/logbooks/fs_afladagbok/*.parquet` | done |
| `afli` | `data-raw/logbooks/01_afli_convert.R` | `data-raw/logbooks/afli/*.parquet` | done |
| `adb` | `data-raw/logbooks/01_adb_convert.R` | `data-raw/logbooks/adb/*.parquet` | not in merge |
| **merged** | `data/logbooks.R` | `data/logbooks/*.parquet` | done |
| **landings match** | `data/logbooks/logbooks.R` (second half) | `data/logbooks/lid_map.parquet` | done |
| **siritar** | `data-raw/logbooks/01_afli_siritar.R` | `data-raw/logbooks/afli/sensor_GPS/` | in progress |
| **xml sensors** | `data-raw/logbooks/01_afladagb_xml.R` | `data-raw/logbooks/afli/sensor_xml_nmea.parquet`, `sensor_xml_track.parquet` | done |

`data-raw/logbooks/01_afli_siritar.R` — AIS-based wacky coordinate recovery for `sjalfvirkir_maelar`/`rafr_sjalfvirkir_maelar`. Interpolates corrected lon/lat from AIS tracks (`data/trail`) onto GPS logger timestamps; retains original wacky coords as `w_lon`/`w_lat`. `dt_sec` is seconds between the two AIS fixes that bracket each GPS timestamp (the actual interpolation interval; `NA` for extrapolated records). `approx(..., rule = 2)`: GPS timestamps outside the AIS window are extrapolated from the nearest AIS endpoint; `NA` lon/lat only for vessels with no AIS in that year.

- `fs_afladagbok` — FisheryScan digital logbooks from Fiskistofa; static dump through 2025-12
- `afli` — legacy Oracle system; primary historical source (~1950–present); 96 tables
- `adb` — live-streaming feed; **excluded from merge** (old gear codes; less reliable coordinates)

Key structural differences across schemas: start/end of fishing activity
recorded differently; numerical gear code systems differ (old vs new).

Seasonality in Icelandic fisheries (relevant for interpreting trip/landing volume patterns):
- **Summer jigger fishery** (LHM) — primary seasonal driver; peaks July–August.
- **Lumpsucker fishery** (Grásleppunet/Rauðmaganet) — secondary peak in spring.

Pelagic catch accounting: common for a vessel to transfer catch at sea; catching
vessel's logbook carries the full haul, landing appears under the receiving vessel.
Trip-level logbook/landing weight ratios are not expected to balance for pelagic trips.
Units in both `catch.parquet` and `aflagrunnur_v` (`magn_oslaegt`) are kg throughout.

---

## AIS pipeline

### Raw data (`data-dump/ais/stk/`)

Dumped from Oracle `stk.trail` by `data-dump/ais/DUMP_stk.R`.

| File/dir | Contents |
|---|---|
| `trail/` | AIS pings partitioned by year; columns `.id`, `mid`, `time`, `lon`, `lat`, `speed`, `heading`, `hid`, `io`, `recdate`, `year` |
| `mobile.parquet` | Maps `mid` → `loid` (local id / call sign) + `glid` (global id / MMSI) |
| `einarhj_mobile-vid.parquet` | Legacy mid→vid matches used as a prior |
| `stk_mobile_match-and-duals.ods` | Manual mid→vid matches with date ranges (the "duals" file); ODS date entries may contain errors — a `pmin`/`pmax` swap guard is applied on load |

Notes on `mid`:
- `mid` is a mobile ID assigned by the STK system; it may not be 1:1 with vessel.
- A `mid` can migrate between vessels (decommissioned vessel → new vessel gets same call sign).
- The same vessel can have multiple `mid`s over time (transponder replacements).

### Commercial AIS pre-processing

Two commercial AIS providers require a ZIP → parquet conversion step before consolidation.

| Script | Input | Output | Notes |
|---|---|---|---|
| `data-raw/ais/astd_zip-to-parquet.R` | `data-dump/ais/astd/*.zip` (monthly) | `data-raw/ais/astd/year=YYYY/month=MM/` | Schema sniff pass resolves cross-month column drift; MM/DD/YYYY timestamp format hard-coded for 2023-11–2024-04; DuckDB COPY, no R memory |
| `data-raw/ais/astdB_zip-to-parquet.R` | `data-dump/ais/astdB/*.zip` (single archive of daily .gz) | `data-raw/ais/astdB/year=YYYY/month=MM/` | readr + duckdbfs::write_dataset; two bad daily files excluded (2019-02-12, 2021-06-12) |

### AIS consolidation

**Script**: `data/ais-data-consolidation.R`  
**Output**: `data/trail/` — Hive-partitioned parquet by year; ~566 million rows (Icelandic vessels, 2007–2026)

Merges five sources: STK (temporal interval join via mid→vid), ASTD (MMSI prefix `251`), ASTDB (cc_iso3 == "ISL"), GPS wacky-recovery positions (`sensor_GPS/`), and EmodNet 2017 snapshot. Harmonises schema to `vid, mmsi, time, lon, lat, speed, heading, cog, provider`. Adds `pid` harbour tag via `ST_INTERSECTS` spatial join. Written by year in a loop via `duckdbfs::write_dataset`; entire pipeline runs in a single R session via DuckDB with no data loaded into memory.

**TIMESTAMP type discipline**: `data/trail` stores `time` as plain `TIMESTAMP` (not `TIMESTAMPTZ`). All source parquets must also be plain `TIMESTAMP`. Use `duckdbfs::write_dataset` (not `arrow::write_parquet` / `nanoparquet::write_parquet`) whenever writing parquet from an R data frame that contains POSIXct timestamp columns — `arrow` and `nanoparquet` both write `TIMESTAMP WITH TIME ZONE`, which DuckDB cannot subtract directly. For EmodNet (`emn`), which enters via `duckdbfs::as_dataset()` on a POSIXct data frame, an explicit `mutate(time = dplyr::sql("time::TIMESTAMP"))` cast is applied before the union.

**Raw data volumes**:

| Source | Rows |
|---|---|
| ASTD (all flags) | ~2.8 billion |
| ASTDB (all flags) | ~901 million |
| EmodNet (Iceland, 2017) | ~19.6 million |
| Consolidated `data/trail` (Icelandic vessels) | ~566 million |

### mid→vid matching

**Script**: `data-raw/ais/stk-mid_vessel-vid_match.R`  
**Output**: `data/vessels/stk_vessel_match.parquet`

Produces one row per (mid, vessel-period) with columns: `mid`, `loid`, `glid`, `pings`, `n_years`, `d1`, `d2`, `vid`, `no`, `step`, `type`, `vid_older`, `mmsi`.

The matching pipeline (stk → stk13) runs through these steps in order:

| Step | Logic |
|---|---|
| `manuals` | Date-range overrides from ODS duals file |
| `vid_vid` | loid == glid, both are Icelandic vessel IDs with MMSI |
| `vid2_vid2` | loid == glid, both are vessel IDs without MMSI |
| `vid_cs` | loid = vessel ID, glid = call sign; cross-checked against `v_mmsi` |
| `uid_cs` | loid = unique ID, glid = call sign |
| `NA_vid` | loid = NA, glid = vessel ID with MMSI |
| `NA_csts` | loid = NA, glid = call sign (broader lookup) |
| `vid2_cs` | loid = vid2 (no MMSI), glid = call sign |
| `older` | Fallback: older legacy matches with > 100 pings |
| `NA_mmsi` | loid = NA/unknown, glid = 9-digit MMSI; matched against `v_mmsi_no_dupes` |

Result (2026-04-25): **3,144 of 31,852** mid records matched to a vid. The remaining ~28,700 are expected non-matches: foreign vessels, land-based AIS receivers, sea-pens, buoys, and pre-AIS Icelandic vessels.

`d2` for the last active period per `mid` is extended to `2028-12-24` (open-ended sentinel). The intended downstream join is:
```r
# Apply fix before joining — see Known data quality issue #10
trip_fixed <- trip |>
  mutate(T2 = pmax(T2, floor_date(T1, "day") + days(1) - seconds(1)))

ais |>
  left_join(stk_vessel_match, by = join_by(mid, between(time, d1, d2))) |>
  left_join(trip_fixed,       by = join_by(vid, between(time, T1, T2)))
```

Known outstanding AIS matching issues (see TODO in script):
- VID 3700–4999 are foreign vessels — no further matching effort planned.
- 3 Icelandic vessels (MMSI 251950001, 251068418, 251898641) are genuine vessels not in the registry; need Fiskistofa lookup.
- ODS date error: mid 101097 (vid 1328) — ODS no=1 dates are both in 2008 (~15-day window); correct to d1=2007-06-01, d2=2023-09-06.

---

## Vessel registry

**File**: `data/vessels/vessels_iceland.parquet`  
**Build script**: `data/vessels/vessel_tidy.R`

Key columns: `vid`, `mmsi`, `cs` (call sign), `uid` (unique ID / registration number), `vessel` (name), `flag`, `source`, `yh1`/`yh2` (vessel active year range), `mclass`, `loa`, `kw`.

- `vid` is the primary vessel key used across logbooks, landings, and AIS.
- `v_mmsi` = subset with non-NA MMSI (used for MMSI-based AIS matching).
- Vessels in range 3700–4999 are **foreign vessels** operating in Icelandic waters.

---

## Landings

**Raw dump**: `data-dump/landings/agf/aflagrunnur.parquet`  
Key columns: `skip_numer` (= `vid`), `londun_hefst` (landing date/time), `magn_oslaegt` (landed weight kg), `ftegund` (species), `hofn` (harbour).

The logbook-to-landings link table is `data/logbooks/lid_map.parquet` (built by the second half of `data/logbooks/logbooks.R`). Harbour codes: `fs_afladagbok` uses `hafnarnumer_id` (1–140); crosswalk via `data/ports/hafnarnumerakerfid.parquet`.

**Cascade match** — eight steps in decreasing key specificity; each step consumes only trips/landings not yet matched; all steps require an unambiguous single `.tid_land` candidate (or the weight/date signal to resolve ambiguity):

| Step | Key | Notes |
|---|---|---|
| 01 exact | vid + date + hid | tightest; highest confidence |
| 02 exact ±1 | vid + date±1 + hid | overnight trips straddling midnight |
| 03 vid_date | vid + date | port dropped; unambiguous vessel-day |
| 04 vid_date ±1 | vid + date±1 | port and date both relaxed |
| 05 vid_date ±2 | vid + date±2 | +~3,400 matches at 11:1 signal-to-ambiguity ratio; all use exactly 2-day offset (systematic recording lag); ±3 drops to ~2:1, not worth it |
| 06 weight_dem | vid + date±2, demersal | catch-weight ratio ≤ `WEIGHT_TOL_DEMERSAL`; gap to runner-up ≥ `WEIGHT_MIN_GAP` |
| 07 weight_pel | vid + date±2, pelagic | same logic, `WEIGHT_TOL_PELAGIC`; expect ~0 matches (catch transfers) |
| 08 expected_date | vid + expected_date±`DATE_ERR_WINDOW` | trips whose recorded date deviates > `DATE_ERR_THRESHOLD` days from tid-interpolated date; ~36 matches but useful QA signal |

Tunable parameters (top of script): `WEIGHT_TOL_DEMERSAL` (`log(1.3)` ≈ 30%), `WEIGHT_TOL_PELAGIC` (`log(2.0)` ≈ 100%), `WEIGHT_MIN_GAP` (`log(1.5)`), `DATE_ERR_THRESHOLD` (35 days), `DATE_ERR_WINDOW` (±3 days).

Step 8 machinery also produces `date_error_days` on unmatched trips — useful QA column for identifying wrong-date entries. `.tid` ordering is chronological for all three ranges: positive afli, negative afli (reversed direction), and fs_afladagbok; one `approxfun` reference curve per range fitted from matched trips.

`gear_mapping` has multiple rows per `gid` across old/new code versions — do not join on `distinct(gid, gear)`; instead derive pelagic flag via `gid %in% pelagic_gids` (gids 7,8,9,10,12,13,19,21,24).

---

## Auxiliary reference tables (`data/aux/` and `data/gear/`)

| File | Contents |
|---|---|
| `data/aux/maritime_identification_digits.parquet` | MID → country; also `MID_child` (98xx) and `MID_aid` (99xx) prefixes for classifying non-vessel MMSI |
| `data/aux/callsign_prefix.parquet` | 2-char prefix → flag state (TF = Iceland filtered out in AIS matching) |
| `data/gear/gear_mapping.parquet` | Old ↔ new gear codes; ICES metier vocabulary |
| `data/ports/hafnarnumerakerfid.parquet` | Port code crosswalk (hafnarnumer_id ↔ hafnarnumer) |
| `data/aux/h3_lookup.parquet` | H3 resolution-9 cell → `ices_area`, `depth_class`, `MSFD_BBHT`; one row per H3 cell; built by `data/DATASET_shapes.R` |

---

## ICES VMS Datacall — Spatial Join Strategy

**Context**: Assigning ~566M AIS pings (`data/trail/`) to three ICES spatial reference objects for the annual VMS datacall.

**Key design principle**: Spatial joins are performed *downstream* of consolidation, on the materialised `data/trail/` parquet. Never embed spatial joins inside the consolidation UNION pipeline — DuckDB's parallel execution makes nested spatial joins non-deterministic.

**Final approach — unified H3 resolution-9 lookup table**: All three spatial objects are expressed as H3 resolution-9 (~174m edge) cell lookups and combined into a single parquet. One DuckDB hash join tags all three variables simultaneously:

```sql
LEFT JOIN read_parquet('data/aux/h3_lookup.parquet') h
  ON geo_to_h3(t.lat, t.lon, 9) = h.h3
```

### Spatial objects and strategies

| Object | Features | Raw vertices | Strategy | Rationale |
|---|---|---|---|---|
| ICES statistical areas (`ia`) | 66 | 1.45 M | `polyfill()` at res 9 | Few features; polygon complexity irrelevant for one-time lookup build; exact coverage, no resolution artefacts |
| ICES GEBCO depth classes | 9 | 7.59 M | `polyfill()` at res 9 | Same as above |
| Seabed habitat (`eusm`) | 2.18 M | ~107 M | Rasterize (0.005°) → H3 res-9 centroid majority vote | 2.18M features makes per-feature polyfill impractical; eusm is a polygonized ~100m raster so rasterizing back is natural |

**Why not polyfill for `eusm`**: Calling `polyfill()` per feature on 2.18M polygons is impractical. The raster intermediary (0.005° ≈ 500m grid) consolidates the polygons into a regular grid; H3 cells are then assigned from raster cell centroids with majority vote to resolve overlaps.

**Why not raster for `ia` / `ICES_GEBCO`**: Polyfill gives exact polygon coverage with no resolution artefact at boundaries. For 66 and 9 features respectively it is trivially fast, and polygon vertex complexity is irrelevant when building the lookup only once.

**On-land tagging** (H3 res-9): Iceland shoreline polygon simplified to ~221 KB WKB, polyfilled at H3 resolution 9 using DuckDB's `h3_polygon_wkt_to_cells_string` (~1.1M cells). AIS pings tagged via hash join on H3 cell ID. Documented in `spatial_test2.R`.

**Reference documents**:
- `data/DATASET_shapes.R` — build script for `data/aux/h3_lookup.parquet`
- `spatial_test2.R` — H3 on-land tagging workflow
- `datacall-strategy.qmd` — full justification (vertex diagnostics, alternatives considered, rationale for each approach)

---

## Relationship to `../fishydata`

| | logbooks | fishydata |
|---|---|---|
| **Role** | QA, documentation, methodological work | Production ETL pipeline |
| **Reads from** | `data-raw/data-dump/` (Oracle parquet dumps) | own `data-raw/` + same Oracle dumps |
| **Outputs** | Quarto HTML site; corrected parquet | `data/logbooks/`, `data/ais/` parquet |

QA findings → eventually reflected in `../fishydata/scripts/12_DATASET_logbooks*.R`.
Do **not** duplicate fishydata schema documentation here; link instead.

---

## Data sources (brief)

Full schema documentation in `AGENTS_data_sources.md`.

- `data-dump/logbooks/fs_afladagbok/` — 8 parquet tables; `ws_veidiferd` (trips), `ws_veidi` (stations), `ws_afli` (catch), plus 5 gear-detail tables
- `data-dump/logbooks/afli/` — 96 tables; key ones: `stofn` (6.9 M stations), `afli` (catch), `toga`, `lineha`, `gildra`, `hringn`; `rafr_sjalfvirkir_maelar` (23.6 M logger positions, **wacky coordinates**); `afladagb_xml_mottaka` (483k XML batch submissions)
- Column renaming via `wk_translate()` using `data/dictionary.parquet` (~154 entries; built by `data-raw/DATASET_dictionary.R`)

---

## Unified logbook output schema (brief)

Full column tables, effort units, and merge details in `AGENTS_output_schema.md`.

- `trip.parquet` — one row per voyage (`.tid`, `vid`, `T1`, `hid1`, `T2`, `hid2`, `n_crew`, `source`, `schema`)
- `station.parquet` — spatial/temporal envelope (`.sid`, `.tid`, `date`, `lon1/lat1/lon2/lat2`, `z1/z2`, `schema`)
- `fishing_sample.parquet` — gear and effort, 1:1 with station (`.sid`, `.tid`, `gid`, `gid_old`, `gear`, `target2`, `t1`–`t4`, `duration_m`, effort columns, `g_mesh/g_width/g_length/g_height`, `back_entry`, `schema`)
- `catch.parquet` — one row per species per station (`.sid`, `sid`, `catch`)

Effort: `effort = effort_count × effort_duration`. Units: `"gear-minutes"`, `"hook-days"`, `"net-days"`, `"jig-hours"`, `"trap-hours"`, `"setting"`.

**Merged output** (afli > fs_afladagbok): 7,260,215 stations · 1,831,690 trips · 16,782,743 catch rows (2026-04-20).

---

## Utility functions (`R/`)

| File | Contents |
|---|---|
| `R/translate_name.R` | `translate_name()` — renames columns using a dictionary; works on data frames and lazy duckdb connections |

---

## Pipeline orchestration (`targets`)

Entry point: `_targets.R`. 14 targets across 4 stages; Stage 1 conversions can run in parallel with `tar_make(workers = N)`.

Directory layout:
- `data-raw/logbooks/` — convert scripts + per-schema parquet outputs
- `data-raw/ais/` — AIS processing scripts
- `data/logbooks/logbooks.R` — merge script + landings match cascade; `data/logbooks/` — merged output
- `data/vessels/` — vessel registry + mid→vid match output
- `data-dump/` — Oracle dump scripts (`logbooks/`, `ais/`, `gear/`, `species/`, `vessels/`, `landings/`)

---

## Coding conventions

- **dplyr-first**: prefer dplyr pipelines; DuckDB/SQL only for performance.
- **Parquet for storage**: `nanoparquet::write_parquet()` or `arrow::write_parquet()` for data without POSIXct columns. For data frames containing POSIXct timestamps, use `duckdbfs::write_dataset()` — both `arrow` and `nanoparquet` write POSIXct as `TIMESTAMP WITH TIME ZONE`, which DuckDB cannot subtract directly.
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

**Navbar** (left-to-right): Home · Grammar · Source Data · Pipeline (Convert/Merge) · Coordinates · Landings Match · Notes · GitHub

| File | Topic |
|---|---|
| `index.qmd` | Non-technical landing page |
| `grammar.qmd` | Grammar of sea-going observational data — four-level hierarchy, unified column vocabulary, time-naming convention; sensor layer; parallel-projects pattern (landings, surveys) |
| `afli-tables.qmd` | Full inventory of all 96 afli tables; schema evolution; wacky coordinate origin |
| `convert-bugs.qmd` | Convert script audit |
| `merge.qmd` | Merge rationale, two-tier method, coverage, timing quality, catch totals |
| `wacky_recovery.qmd` | Wacky coordinate discovery, mechanism, recovery, appendices |
| `afli-backentry.qmd` | Historical back-entry investigation; `back_entry` flag derivation |
| `landings-match.qmd` | Logbook-to-landings linking; eight-step cascade logic; match rates |
| `on_ais.qmd` | AIS data sources, pre-processing pipeline, data volumes, and commentary on DuckDB vs cloud architecture (WGSFD 2025) |
| `ramb-list.qmd` | Informal notes |

---

## Institutional context (afli era)

- **E-logbook adoption** happened during the `afli` phase. Larger vessels adopted Trackwell progressively; smaller vessels (lumpfish, coastal) remained on paper.
- **Single curator**: one government employee manually entered paper logbooks. His retirement ~2019–2020 explains the subsequent decline in match rates within `afli`.
- **Spring 2021 market opening**: abrupt end of Trackwell's exclusive contract → "wild west" period explains data quality issues in `fs_afladagbok`. Primary justification for **afli wins** in the merge overlap (2021–2022).

---

## Known data quality issues

1. **Wacky coordinates** — DDMMmm→DMS misconversion in `sjalfvirkir_maelar`/`rafr_sjalfvirkir_maelar`; shifts ≤ ~0.4 NM. AIS-based recovery in `01_afli_siritar.R` (in progress).
2. **Erroneous timestamps** — year-1899 and year-2090s in `rafr_sjalfvirkir_maelar`; Oracle null/default values.
3. **Historical back-entries** — 95,743 stations (~1.7%) with pre-1980 dates but `.sid > 2.5M`; flagged `back_entry = TRUE`.
4. **Residual t1 ≤ t4 violations** — small number in `afli`; filter `.sid > 0` before timing-sensitive analyses.
5. **DRB duration suspiciously short** — `fs_afladagbok` DRB median ~18 min; not investigated.
6. **~54k OTB stations (2021–2022) with no `ws_dragnot_varpa`** — `effort_unit = NA`; source gap in `fs_afladagbok`.
7. **`eytt_deleted` records (327 stations)** — flagged `eytt_deleted = TRUE` in `station` and `fishing_sample`.
8. **Two negative-visir pathways in `stofn`** — (a) `rafr_*` pipeline 2003–2020; (b) direct-load 2020–2022. Both use DDMM encoding and old gear codes.
9. **AIS mid→vid overlaps** — 35 vids have overlapping mid periods in `stk_vessel_match.parquet`; all are transponder-changeover bleed or simultaneous dual-broadcasting, not pipeline errors.
10. **Trip T1/T2 timing precision** — two schema-specific issues that break `between(time, T1, T2)` joins:
    - `afli`: ~80% of trips have `T1 == T2` (date-midnight), creating a zero-length window. Fix: extend T2 to 23:59:59 when `T1 == T2`.
    - `fs_afladagbok`: T2 is stored as date-midnight of the *arrival* date; for same-day trips this makes `T2 < T1`. Fix: push T2 to end-of-day of T1's date when `T2 < T1`.
    - Both cases handled by: `mutate(T2 = pmax(T2, floor_date(T1, "day") + days(1) - seconds(1)))`. After fix, `afli` trip-to-AIS match rate ~98%; `fs_afladagbok` baseline was ~75% (pre-fix).

---

## Outstanding Work

- [ ] **Lumpfish logbook QA** — gid=3/4 match rates 26%/50%; needs investigation before catch reconciliation.
- [ ] **Finalise `01_afli_siritar.R`** — run at full scale; write `sensor_GPS/` (directory, via `duckdbfs::write_dataset`); notify `../fishydata`.
- [ ] **Characterise AIS coverage gaps** — quantify NA lon/lat from `rule = 2` extrapolation.
- [ ] **Fix gid 9 coordinate encoding** in `01_fs_afladagbok_convert.R` — classify Flotvarpa rows by `uppruni`.
- [ ] **Confirm longline `effort_count` semantics** — is `fj_kroka` total hooks or number of lines?
- [ ] **Fix DRB `effort_count` in `01_afli_convert.R`** — hardcode to `1L` for post-2011 rows.
- [ ] **Fix ODS date error: mid 101097** — correct no=1 to d1=2007-06-01, d2=2023-09-06 in `stk_mobile_match-and-duals.ods`.
- [ ] **Unmatched Icelandic MMSI vessels** — look up in Fiskistofa registry: mid 114902 (251950001), mid 118765 (251068418), mid 139879 (251898641).
- [ ] **ICES VMS datacall — run `data/DATASET_shapes.R`** — execute to build `data/aux/h3_lookup.parquet` (polyfill `ia`/`ICES_GEBCO`, rasterize+H3 `eusm`).
- [ ] **ICES VMS datacall — integrate into main pipeline** — apply `data/aux/h3_lookup.parquet` hash join to full `data/trail/` dataset (all years 2007–2026); test end-to-end.

### Completed

- [x] **ICES VMS datacall spatial join strategy finalised (2026-04-30)** — unified H3 resolution-9 lookup table combining all three spatial objects into `data/aux/h3_lookup.parquet`; `polyfill()` for `ia` (66 features) and `ICES_GEBCO` (9 features); rasterize (0.005°) → H3 centroid majority vote for `eusm` (2.18M features, polygonized raster); `data/DATASET_shapes.R` and `datacall-strategy.qmd` updated; `DATASET_shapes-simplify.R` removed.
- [x] **AIS whacky-point detection implemented (2026-04-27)** — `wack_points.R`; `whack_fwdbwd(x)` dispatches on `data.frame` vs `tbl_lazy`; forward-backward speed filter (25 kn threshold); validated against `ramb::whacks1`; `whack_sequential_fast()` provided for cluster cases. TIMESTAMPTZ issue traced and fixed: `sensor_GPS` now written via `duckdbfs::write_dataset`, `emn` cast with `time::TIMESTAMP`, consolidation write switched to `duckdbfs::write_dataset`.
- [x] **AIS consolidation pipeline documented (2026-04-26)** — `on_ais.qmd` written; `data/ais-data-consolidation.R` commented; `data-raw/ais/astd_zip-to-parquet.R` and `astdB_zip-to-parquet.R` described; data volumes recorded (~2.8B ASTD, ~901M ASTDB, ~19.6M EmodNet, ~566M consolidated).
- [x] **mid→vid matching pipeline completed (2026-04-25)** — `data-raw/ais/stk-mid_vessel-vid_match.R`; output `data/vessels/stk_vessel_match.parquet`; 3,144/31,852 mids matched; added `vid2_vid2` step; two non-vessel MMSIs added to `fixed` list; ODS swap guard added.
- [x] **t0–t3 → t1–t4 reset (2026-04-22)** — all convert scripts, dictionary, grammar.qmd, merge.qmd, AGENTS_output_schema.md updated.
- [x] **DRB `gid_old = 38` spike investigated (2026-04-22)** — confirmed genuine 2017–2018 grenadier plow fishery; no pipeline action.
- [x] `rafr_` vs `stofn` strategic review (2026-04-22) — master tables authoritative; `eytt_deleted` flag added.
- [x] `grammar.qmd` updated (2026-04-21) — sensor layer + parallel-projects sections added.
- [x] `data-raw/logbooks/01_afli_convert.R` audited, fixed, rewritten (2026-04-11/20).
- [x] `data-raw/logbooks/01_fs_afladagbok_convert.R` rewritten (2026-04-20).
- [x] `data/logbooks.R` refactored to two-tier (2026-04-20) — 7,260,215 stations, 1,831,690 trips, 16,782,743 catch rows.
- [x] `merge.qmd` rewritten (2026-04-20).
- [x] `_targets.R` created (2026-04-12) — 14 targets, 4 tiers.
- [x] `data-raw/DATASET_dictionary.R` extended (2026-04-19/20) — 154 entries.
- [x] `afli-tables.qmd` written (2026-04-19).
- [x] XML sensor scripts written (2026-04-19).
- [x] `_quarto.yml` navbar reorganised (2026-04-19).
- [x] Wacky coordinate documents merged (2026-04-19).
