# logbooks — Raw data source schemas

> Load this file when working on raw data, convert scripts, or the dictionary.

Raw data live in `data-raw/data-dump/` and are **gitignored**. Dumped from
Oracle using `data-raw/data-dump/oracle_dump.R`.

---

## `fs_afladagbok/` — FisheryScan digital logbooks (static dump)

Modern electronic submission format from Fiskistofa; records through 2025-12.
Converted by `scripts/01_fs_afladagbok_convert.R`.

| Table (parquet) | Key columns | Description |
|---|---|---|
| `ws_veidiferd.parquet` | `id` (→ `.tid`), `skipnr`, `upphafstimi`, `londunardagur` | Trip records; `id` is the primary key, renamed to `.tid` |
| `ws_veidi.parquet` | `id` (→ `.sid`), `veidiferd_id` (→ `.tid`), `veidarfaeri_id` | Fishing event records; `id` primary key, `veidiferd_id` links to trip |
| `ws_afli.parquet` | `veidi_id` (→ `.sid`), `tegund_id` (→ `sid`), `afli` (→ `catch`) | Catch by species |
| `ws_dragnot_varpa.parquet` | `veidi_id` (→ `.sid`), `grandarar_lengd` (→ `g_width`) | Mobile gear (trawl, dragnót) — gear width; covers gid 1–9, 11 |
| `ws_plogur.parquet` | `veidi_id` (→ `.sid`), `breidd` (→ `g_width`) | Dredge (plógur) — plow width; gid 15 only |
| `ws_linanethandf.parquet` | `veidi_id` (→ `.sid`), `fj_kroka` (→ `n_hooks`), `fj_dreginna_neta` (→ `n_nets`), `fj_faera` (→ `n_jigs`) | Static gear (longline, gillnet, handline); gid 1–5, 12, 14, 22 |
| `ws_gildra.parquet` | `veidi_id` (→ `.sid`), `fj_gildra` (→ `n_units`) | Trap records |
| `ws_hringn.parquet` | `veidi_id` (→ `.sid`) | Purse seine / ring net records |

**Coordinates** in `ws_veidi.parquet` are stored as integers in either DMS
(`DD×10000 + MM×100 + SS`) or DDM (`DD×10000 + MM.CC×100`) format depending on
the data source (`uppruni`). The convert script classifies each trip as `"dms"`
or `"ddm"` using a row-level signal (last-2-digits ≥ 60 is impossible in DMS)
lifted to trip level, combined with a hard list of known DDM sources
(`DDM_SOURCES`). Conversion uses `wk_convert_dms()` / `wk_convert_ddm()` from
the `whack` package.

**Known coordinate limitation:** gid 9 (Flotvarpa / midwater trawl) rows in
`ws_veidi.parquet` use mixed decimal-degree (1–2 digit integers) and DMS (5–6
digit integers) encoding that cannot be disentangled without per-source
(`uppruni`) classification. 639 stations retain incorrect positive longitudes.

---

## `afli/` — legacy Oracle logbook system (primary)

96 tables spanning five decades. Full inventory in `afli-tables.qmd`. Dump
includes metadata files `_overview_tables.parquet` and `_overview_fields.parquet`.

**Three schema eras:**

| Era | Tables | Date range |
|---|---|---|
| Paper logbooks | `stofn`, `afli`, `toga`, `lineha`, `gildra`, `hringn`, … | ~1950–present |
| Early electronic submissions | `rafr_stofn`, `rafr_afli`, `rafr_toga`, … | ~2000–2022 |
| Mandatory digital + compliance | `afladagb_skil`/`_linur`, `afladagb_xml_mottaka` | 2014–2021 |

Converted by `scripts/01_afli_convert.R`. Coordinates in `stofn.parquet` stored
in DDMM integer format; converted via `geo::geoconvert.1()` (×100 before passing
in; longitude negated for West).

**Tables used by `scripts/01_afli_convert.R`:**

| Table (parquet) | Description |
|---|---|
| `stofn.parquet` | Fishing station/event records; `visir` is the unique key; 6.9 M rows |
| `afli.parquet` | Catch records by species |
| `toga.parquet` | Tow/mobile gear records |
| `lineha.parquet` | Longline and gillnet records |
| `gildra.parquet` | Trap records |
| `hringn.parquet` | Purse seine / ring net records |

**Additional key tables (not used by convert script):**

| Table (parquet) | Rows | Description |
|---|---|---|
| `rafr_stofn.parquet` | 1,149,278 | Electronic-era fishing stations; carries `xml_sending_visir` → `afladagb_xml_mottaka.id` back-link; coordinates as DDMM integers |
| `rafr_afli.parquet` | 3,625,502 | Electronic-era catch records |
| `rafr_toga.parquet` | 931,260 | Electronic-era trawl records |
| `rafr_lineha.parquet` | 209,257 | Electronic-era longline/gillnet records |
| `sjalfvirkir_maelar.parquet` | 28,234,223 | Automatic logger positions — older, wider (incl. `vindhradi`/`vindstefna`); **wacky coordinates** |
| `rafr_sjalfvirkir_maelar.parquet` | 23,610,639 | Automatic logger positions — leaner; **identical coordinate values** to `sjalfvirkir_maelar` for 685,656 overlapping `visir`; **wacky coordinates** |
| `afladagb_xml_mottaka.parquet` | 483,738 | Raw XML submissions; `sending` column holds full XML; software (`hugbunadur`) predominantly Trackwell variants |
| `afladagb_skil.parquet` | 9,961 | Compliance master table (from Jan 2014) |
| `afladagb_skil_linur.parquet` | 46,561 | Compliance detail by fishing month |
| `rafr_hledsla_log.parquet` | 6,595,396 | Oracle import run log; Icelandic-language messages |

**Raw column vocabulary (afli system → unified name):**

| Raw | Unified | Meaning |
|---|---|---|
| `visir` | `.sid` | Station identifier |
| `skipnr` | `vid` | Vessel identifier |
| `veidarf` | `gid` | Gear type code |
| `dags` | `date` | Fishing date |
| `tegund` | `sid` | Species code |
| `magn` | `catch` | Catch amount |
| `timi` | `timi` | Timestamp (logger positions) |
| `lengd` | `lengd` | Longitude — logger positions (**wacky error**; see `wacky_recovery.qmd`) |
| `breidd` | `breidd` | Latitude — logger positions (**wacky error**; see `wacky_recovery.qmd`) |
| `skip_hradi` | `skip_hradi` | Instantaneous vessel speed (knots) |
| `skip_stefna` | `skip_stefna` | Instantaneous vessel heading (degrees) |

---

## `gear/` — gear code vocabulary (hand-maintained)

Script: `data-raw/DATASET_gear-codes.R`  
Output: `data/gear/gear_mapping.parquet`  
Reference: `data-raw/data-dump/gear/asfis.parquet` (loaded but not yet used)

The `gear_mapping` table maps codes between the two schemas and assigns ICES
metier vocabulary. One row per gear per version.

| Column | Description |
|---|---|
| `version` | `"old"` (afli) or `"new"` (fs_afladagbok) |
| `gid` | Gear code in this version |
| `veiðarfæri` | Icelandic gear name |
| `map` | Canonical counterpart gid in the **other** version |
| `gear` | ICES GearType code (e.g. `"OTB"`, `"LLS"`) |
| `target` | ICES TargetAssemblage code (e.g. `"DEF"`, `"SPF"`) |
| `target2` | Custom target aligned with Icelandic fisheries (partially FAO ASFIS; not ICES-validated) |

**`map` semantics.** One-way canonical lookup only. The mapping is many-to-one
in both directions (e.g. five midwater trawl sub-types in old → one "Flotvarpa"
in new). Round-trips are lossy. Do **not** rely on `map` for bijective
conversion.

Old-side codes 91 (Skötuselsnet), 92 (Grálúðunet), and 99 (Óskráð veiðarfæri)
are explicit rows in `gear_mapping` for new→old lookup completeness; they do
not appear in actual `afli` logbook data (`stofn.parquet`).

---

## `adb/` — live-streaming feed (in-house conversion)

Current live feed from Fiskistofa, converted in-house to partially emulate the
historical `afli` format. Data content effectively the same as `fs_afladagbok`,
just restructured differently. **Uses the old (afli-era) gear code system** —
gear codes stored in `gid_old` in converted output; `gid` derived via
`gear_mapping` (old→new) at merge time.

Converted by `scripts/01_adb_convert.R` → `data/adb/*.parquet`.

---

## `logbook/` — experimental restructuring (not in use)

Experimental re-layout of `fs_afladagbok`. Not actively developed; no
conversion script planned.

| Table (parquet) | Description |
|---|---|
| `station.parquet` | Fishing stations/events |
| `catch.parquet` | Catch by species |
| `trawl.parquet` | Trawl-specific fields |
| `hook_line.parquet` | Hook-and-line-specific fields |
| `seine_net.parquet` | Seine net records |
| `trip.parquet` | Trip/voyage metadata |
