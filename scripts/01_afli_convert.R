# Create standard summary tables
# Input:  data-raw/data-dump/afli/{stofn,toga,lineha,gildra,hringn,afli,rafr_stofn}.parquet
# Output: data/afli/{trip,station,fishing_sample,sensor,catch,aggregate}.parquet

# Setup -----------------------------------------------------------------------
library(whack) # pak::pak("einarhjorleifsson/whack")
library(geo)
library(tidyverse)
library(nanoparquet)

SCHEMA <- "afli"
dictionary   <- read_parquet("data/dictionary.parquet") |> filter(schema == SCHEMA)
gear_mapping <- read_parquet("data/gear/gear_mapping.parquet") |> filter(version == "old")

# eytt flag: records officially deleted in Oracle (eytt=1 in rafr_stofn).
# 16,912 such records exist; they were imported into stofn but later deleted.
# eytt_deleted = TRUE  → deleted in Oracle; treat with caution / exclude.
# eytt_deleted = FALSE → active in Oracle (rafr_ era, 2003–2020).
# eytt_deleted = NA    → not in rafr_ (paper era, or 2020–2022 direct-load).
eytt_ref <- read_parquet("data-raw/data-dump/afli/rafr_stofn.parquet") |>
  select(visir, eytt) |>
  mutate(eytt_deleted = eytt == 1L) |>
  select(visir, eytt_deleted)

# Source (stofn) --------------------------------------------------------------
# No explicit trip table in this schema; .tid derived as min(.sid) per
# (vid, T2, hid2). T1 is the minimum fishing date within the derived trip.
source <-
  read_parquet("data-raw/data-dump/afli/stofn.parquet") |>
  wk_translate(dictionary) |>
  filter(!is.na(date)) |>                        # 40 records have NA date
  # Remove ~41k monthly aggregate records (nephrops, shrimp, d-seine); these
  # are summaries at month-gear-vessel-square level, not individual hauls
  filter(
    is.na(comment) |
    (!comment %in% c("Frá OPS$SIGFUS.GAMALL_HUMAR, samantekt",
                     "Frá OPS$SIGFUS.GOMUL_RAEKJA, samantekt") &
     !str_starts(comment, "Frá afli.gomul_dragnot, samantekt"))
  ) |>
  mutate(lon1 = -geoconvert.1(x1 * 100),
         lat1 =  geoconvert.1(y1 * 100),
         lon2 = -geoconvert.1(x2 * 100),
         lat2 =  geoconvert.1(y2 * 100)) |>
  group_by(vid, T2, hid2) |>
  mutate(.tid = min(.sid)) |>
  group_by(vid, .tid) |>
  mutate(T1 = min(date, na.rm = TRUE), hid1 = NA_integer_) |>
  ungroup() |>
  mutate(schema = SCHEMA) |>
  left_join(eytt_ref, by = c(".sid" = "visir")) |>
  select(.sid, .tid, vid, gid, date, lon1, lat1, lon2, lat2, sq, ssq, z1, z2,
         T1, hid1, T2, hid2, n_crew, schema, eytt_deleted)

# Helper: station keys with ICES gear labels, used in aux table joins below
stofn_gear <- source |>
  select(.sid, gid) |>
  left_join(gear_mapping |> select(gid, gear), by = "gid")

# Trip ------------------------------------------------------------------------
# Derived: one row per (vid, T2, hid2) group
trip <-
  source |>
  select(.tid, vid, T1, hid1, T2, hid2, n_crew, schema) |>
  distinct(.tid, vid, T1, hid1, T2, hid2, .keep_all = TRUE)

# Station (spatial/temporal envelope) -----------------------------------------
station <-
  source |>
  select(.sid, .tid, date, lon1, lat1, lon2, lat2, sq, ssq, z1, z2, schema,
         eytt_deleted)

# Aux tables ------------------------------------------------------------------
# Each block reads one source parquet, translates columns, and inner-joins to
# the relevant gear class. All downstream fishing_sample, sensor, and dims
# content is extracted from these four objects — no further sub-objects.

## Mobile (toga: OTB / OTM / DRB / SDN) ---------------------------------------
aux_mobile <-
  read_parquet("data-raw/data-dump/afli/toga.parquet") |>
  wk_translate(dictionary) |>
  inner_join(stofn_gear |> filter(gear %in% c("DRB", "OTB", "OTM", "SDN")),
             by = ".sid")

## Static (lineha: LLS / GNS / GND / LHM) -------------------------------------
aux_static <-
  read_parquet("data-raw/data-dump/afli/lineha.parquet") |>
  rename(.sid = visir) |>
  wk_translate(dictionary) |>
  inner_join(stofn_gear |> filter(gear %in% c("LLS", "GNS", "GND", "LHM")),
             by = ".sid")

## Trap (gildra: FPO) ----------------------------------------------------------
aux_trap <-
  read_parquet("data-raw/data-dump/afli/gildra.parquet") |>
  wk_translate(dictionary) |>
  inner_join(stofn_gear |> filter(gear == "FPO"), by = ".sid") |>
  mutate(duration_m = duration_h * 60)

## Seine / ring net (hringn: PS) -----------------------------------------------
aux_seine <-
  read_parquet("data-raw/data-dump/afli/hringn.parquet") |>
  wk_translate(dictionary) |>
  inner_join(stofn_gear |> filter(gear == "PS"), by = ".sid")

# Fishing sample --------------------------------------------------------------

## Timing ----------------------------------------------------------------------
# Event timestamps and soak/tow duration from each aux table.
# Grammar convention for static gear: Icelandic t1 (retrieval start) → t2;
# Icelandic t2 (retrieval end) → t3. t1 (deployment end) is unrecorded for
# static gear. Mobile gear uses t1 (tow start) and t2 (tow end) as-is.
timing <-
  bind_rows(
    aux_mobile |> select(.sid, hhmm, duration_m),
    aux_static |>
      mutate(duration_m = case_when(!is.na(duration_d) ~ duration_d * 24 * 60,
                                    !is.na(duration_h) ~ duration_h * 60)) |>
      select(.sid, t0, t2 = t1, t3 = t2, duration_m),
    aux_trap   |> select(.sid, duration_m),
    aux_seine  |> select(.sid, hhmm)
  ) |>
  filter(!if_all(-.sid, is.na))

## Gear dimensions + effort inputs ---------------------------------------------
# One row per station; columns vary by gear class (NA elsewhere). effort_count
# and effort_unit are derived here alongside gear dimensions — they share the
# same source columns (hook counts, net counts, trap counts, etc.).
dims <-
  bind_rows(
    # OTB / OTM — gear dimension documentation pending
    aux_mobile |>
      filter(gear %in% c("OTB", "OTM")) |>
      # I think we can assume that this is always 1
      mutate(effort_count = 1L,
             effort_unit  = "gear-minutes") |>
      select(.sid, n_units, effort_count, effort_unit, bottom_type, catch_nperkg),
    # DRB — dredge / plow
    aux_mobile |>
      filter(gear == "DRB") |>
      mutate(effort_count = coalesce(as.integer(n_units), 1L),
             effort_unit  = "gear-minutes") |>
      select(.sid, n_units, effort_count, effort_unit,
             g_mesh = mesh, g_width = width_plow, g_height = height_plow, g_length = length_plow,
             bottom_type, catch_nperkg),
    # SDN — Danish seine (effort is per setting, not time-based)
    aux_mobile |>
      filter(gear == "SDN") |>
      mutate(effort_count = 1L, effort_unit = "setting") |>
      select(.sid, effort_count, effort_unit,
             g_mesh = mesh, length_togle, length_virle, mesh_vod, length_headline, length_fotreipi),
    # LLS — longline
    aux_static |>
      filter(gear == "LLS") |>
      mutate(n_total = case_when(!is.na(n_hooks)                          ~ n_hooks,
                                 !is.na(n_hooks_per_set) & !is.na(n_sets) ~ n_hooks_per_set * n_sets),
             effort_count = n_total,
             effort_unit  = "hook-days") |>
      filter(!if_all(c(n_hooks, n_hooks_per_set, n_sets), is.na)) |>
      select(.sid, n_total, n_per_unit = n_hooks_per_set, n_units = n_sets,
             effort_count, effort_unit),
    # GNS / GND — gillnet
    aux_static |>
      filter(gear %in% c("GNS", "GND")) |>
      mutate(n_units      = n_nets,
             length       = n_nets * mean_gillnet_length,
             effort_count = n_units,
             effort_unit  = "net-days") |>
      select(.sid, n_units, g_mesh = mesh, g_height, g_length = length, n_lost,
             effort_count, effort_unit),
    # LHM — handline / jig
    aux_static |>
      filter(gear == "LHM") |>
      mutate(effort_count = n_jigs, effort_unit = "jig-hours") |>
      select(.sid, n_units = n_jigs, n_per_unit = n_hooks,
             effort_count, effort_unit),
    # FPO — traps
    aux_trap |>
      mutate(effort_count = n_units, effort_unit = "trap-hours") |>
      select(.sid, n_units, effort_count, effort_unit),
    # PS — purse seine / ring net (effort is per set)
    aux_seine |>
      mutate(effort_count = 1L, effort_unit = "setting") |>
      select(.sid, effort_count, effort_unit,
             g_mesh = mesh, g_length, g_height)
  )


## Gear caps -------------------------------------------------------------------
# Maximum plausible fishing duration per gear × target combination (minutes).
gear_caps <- tribble(
  ~gear,  ~target2,  ~cap_m,
  "OTB",  "DEF",      10 * 60,   # finfish
  "OTB",  "NEP",       8 * 60,   # Nephrops
  "OTB",  "SHR",      16 * 60,   # shrimp
  "OTM",  "SPF",      24 * 60,   # pelagics
  "OTM",  "XXX",      24 * 60,
  "DRB",  "ARI",       4 * 60,   # suspiciously low — CHECK
  "DRB",  "ECH",      12 * 60,
  "DRB",  "SCL",      12 * 60,
  "SDN",  "DEF",       3 * 60,
  "LLS",  "DEF",      24 * 60,
  "LHM",  "DEF",      24 * 60,
  "GNS",  "DEF",   7 * 24 * 60,
  "GNS",  "LUM",   7 * 24 * 60,
  "GNS",  "MON",   7 * 24 * 60,
  "GNS",  "GHL",   7 * 24 * 60,
  "GND",  "SPF",           60,
  "FPO",  "GAD",  20 * 24 * 60,
  "FPO",  "CRB",  20 * 24 * 60,
  "FPO",  "WHL",  20 * 24 * 60
)

## Assemble fishing_sample -----------------------------------------------------
fishing_sample <-
  source |>
  select(.tid, .sid, gid, date, schema, eytt_deleted) |>
  # Old → new gear code mapping done once here
  rename(gid_old = gid) |>
  left_join(gear_mapping |> select(gid_old = gid, gid = map, gear, target2),
            by = "gid_old") |>
  left_join(timing, by = ".sid") |>
  # OTB NEP pre-2000: duration appears to be recorded in hours not minutes
  mutate(duration_m = if_else(gear == "OTB" & target2 == "NEP" & year(date) <= 1999,
                              NA_real_, duration_m)) |>
  # Apply duration caps
  left_join(gear_caps, by = c("gear", "target2")) |>
  mutate(.duration_source = case_when(is.na(duration_m) ~ "missing",
                                      duration_m > cap_m ~ "capped",
                                      .default           = "data"),
         duration_m       = case_when(is.na(duration_m) ~ NA_real_,
                                      duration_m > cap_m ~ cap_m,
                                      .default           = duration_m)) |>
  select(-cap_m) |>
  # Effort: two-component — effort = effort_count × effort_duration
  left_join(dims |> select(.sid, effort_count, effort_unit), by = ".sid") |>
  mutate(effort_duration = case_when(
    effort_unit == "gear-minutes" ~ duration_m,
    effort_unit == "hook-days"    ~ duration_m / (60 * 24),
    effort_unit == "net-days"     ~ duration_m / (60 * 24),
    effort_unit == "jig-hours"    ~ duration_m / 60,
    effort_unit == "trap-hours"   ~ duration_m / 60,
    effort_unit == "setting"      ~ 1,
    .default = NA_real_
  ),
  effort = effort_count * effort_duration)


## Timestamps ------------------------------------------------------------------
# Mobile: derive t1 from hhmm when absent; derive t2 from t1 + duration_m.
# Static: t2/t3 come from data; t1 is never recorded (remains NA).
fishing_sample <- fishing_sample |>
  mutate(t1 = NA_POSIXct_) |>
  mutate(.t1_source = if_else(!is.na(t1), "data", "derived"),
         .t2_source = if_else(!is.na(t2), "data", "derived"),
         duration_m = if_else(duration_m <= 0, NA_real_, duration_m),
         hhmm = if_else(!is.na(hhmm) & hhmm >= 100, hhmm, NA_real_),
         hh   = as.integer(hhmm %/% 100),
         hh   = if_else(hh < 24, hh, NA_integer_),
         mm   = as.integer(hhmm %% 100),
         mm   = if_else(mm < 60 & !is.na(hh), mm, NA_integer_),
         t1   = case_when(!is.na(t1)              ~ t1,
                          !is.na(hh) & !is.na(mm) ~ as_datetime(date) + hours(hh) + minutes(mm)),
         t2   = case_when(!is.na(t2)                        ~ t2,
                          !is.na(t1) & !is.na(duration_m)   ~ t1 + minutes(as.integer(duration_m)))) |>
  select(-c(hhmm, hh, mm))


## Overlap QC ------------------------------------------------------------------
# Nullify t1/t2 for operations whose windows overlap a neighbour on the same
# vessel. Effectively applies to mobile gear only — static-gear t1 is NA so
# those rows pass through with no change.
fishing_sample <- fishing_sample |>
  inner_join(trip |> select(.tid, vid) |> distinct(), by = ".tid") |>
  arrange(vid, date, t1) |>
  osfd::fd_flag_time_overlaps(t1, t2, by = vid) |>
  mutate(t1 = if_else(.checks != "ok", NA, t1),
         t2 = if_else(.checks != "ok", NA, t2)) |>
  select(-vid)


## Join gear dimensions --------------------------------------------------------
fishing_sample <- fishing_sample |>
  left_join(dims |> select(-effort_count, -effort_unit), by = ".sid")


## Back-entry flag -------------------------------------------------------------
# TRUE for records with pre-1980 dates AND .sid > 2,500,000.
# Rationale: genuine contemporaneous early records (1969–1979) have .sid ≤ 2.34M;
# all pre-1980 records above that threshold form identified retrospective
# back-entry batches. See afli-backentry.qmd for full derivation.
fishing_sample <- fishing_sample |>
  mutate(back_entry = !is.na(date) & year(date) < 1980 & .sid > 2500000)

fishing_sample <- fishing_sample |>
  select(-date)
# Sensor -----------------------------------------------------------------------
sensor <-
  bind_rows(
    aux_mobile |>
      select(.sid, gid, gear,
             flag_hoflinum, flag_nemi, sensor_type,
             temp1_surface, temp2_surface, temp1_bottom, temp2_bottom,
             temp1_headline, temp2_headline, z1_headline, z2_headline),
    aux_static |>
      select(.sid, gid, gear, temp1_air, temp1_surface, temp1_bottom, seastate),
    aux_seine  |>
      select(.sid, gid, gear, seastate)
  ) |>
  filter(!if_all(-c(.sid, gid, gear), is.na))

# Catch -----------------------------------------------------------------------
catch <-
  read_parquet("data-raw/data-dump/afli/afli.parquet") |>
  wk_translate(dictionary, "messy", "clean") |>
  select(.sid, sid, catch) |>
  group_by(.sid, sid) |>
  summarise(catch = sum(catch, na.rm = TRUE), .groups = "drop") |>
  inner_join(fishing_sample |> select(.sid, schema), by = ".sid") |>
  arrange(.sid, sid)

# Aggregate records (monthly summaries, not individual hauls) -----------------
# Excluded from station/fishing_sample but preserved as a flat catch table:
# one row per (period × gear × vessel × sq × species).
# date is mid-month (15th); no coordinates available; duration_m is total
# aggregated tow time for the period (not a single-haul value).
agg_stofn <-
  read_parquet("data-raw/data-dump/afli/stofn.parquet") |>
  wk_translate(dictionary) |>
  filter(!is.na(date)) |>
  filter(
    comment %in% c("Frá OPS$SIGFUS.GAMALL_HUMAR, samantekt",
                   "Frá OPS$SIGFUS.GOMUL_RAEKJA, samantekt") |
    str_starts(comment, "Frá afli.gomul_dragnot, samantekt")
  ) |>
  mutate(
    agg_type = case_when(
      comment == "Frá OPS$SIGFUS.GAMALL_HUMAR, samantekt"      ~ "nephrops",
      comment == "Frá OPS$SIGFUS.GOMUL_RAEKJA, samantekt"      ~ "shrimp",
      str_starts(comment, "Frá afli.gomul_dragnot, samantekt") ~ "dseine"
    )
  ) |>
  select(.sid, vid, gid, date, sq, ssq, agg_type)

agg_catch <-
  read_parquet("data-raw/data-dump/afli/afli.parquet") |>
  wk_translate(dictionary, "messy", "clean") |>
  select(.sid, sid, catch) |>
  inner_join(agg_stofn, by = ".sid") |>
  left_join(
    read_parquet("data-raw/data-dump/afli/toga.parquet") |>
      wk_translate(dictionary) |>
      select(.sid, duration_m),
    by = ".sid"
  ) |>
  select(.sid, vid, gid, date, sq, ssq, agg_type, sid, catch, duration_m) |>
  arrange(.sid, sid)


# Export -----------------------------------------------------------------------
trip           |> write_parquet("data/afli/trip.parquet")
station        |> write_parquet("data/afli/station.parquet")
fishing_sample |> write_parquet("data/afli/fishing_sample.parquet")
sensor         |> write_parquet("data/afli/sensor.parquet")
catch          |> write_parquet("data/afli/catch.parquet")
agg_catch      |> write_parquet("data/afli/aggregate.parquet")


# QC scratch (if FALSE) -------------------------------------------------------
if (FALSE) {

  ## Duration distributions by gear type ---------------------------------------
  fishing_sample |>
    ggplot(aes(duration_m / 60, fill = target2)) +
    geom_histogram() +
    facet_wrap(~ gear, scales = "free") +
    scale_fill_brewer(palette = "Set1")

  ## t1/t2 source breakdown ----------------------------------------------------
  fishing_sample |> count(.t1_source, .t2_source)

  fishing_sample |>
    ggplot(aes(duration_m / 60, fill = .t1_source)) +
    geom_histogram() +
    facet_wrap(~ gear, scales = "free") +
    scale_fill_brewer(palette = "Set1")

  ## E-book vs paper logbook over time ----------------------------------------
  fishing_sample |>
    mutate(.ebook = if_else(.sid < 0, "yes", "no"),
           year   = year(date)) |>
    count(year, .ebook, gear) |>
    filter(year >= 1973) |>
    ggplot(aes(year, n, colour = .ebook)) +
    geom_point() +
    facet_wrap(~ gear, scales = "free_y")

  ## Overlap check breakdown ---------------------------------------------------
  fishing_sample |>
    count(.t1_source, .checks) |>
    group_by(.t1_source) |>
    mutate(p = n / sum(n)) |>
    ungroup()

  ## Cross-check against landings ----------------------------------------------
  lods_londun <-
    read_parquet("../landings/data-raw/data-dump/kvoti/lods_londun.parquet") |>
    select(.lid = komunr, vid = skip_nr, D2 = l_dags, hid2 = hofn,
           comment = aths, stada) |>
    mutate(D2 = as_date(D2))
  lods_londun_distinct <-
    lods_londun |>
    filter(vid != 9999) |>
    arrange(vid, D2, hid2, .lid) |>
    distinct(vid, D2, hid2, .keep_all = TRUE)
  trip |>
    mutate(D2 = as_date(T2)) |>
    filter(between(D2, ymd("1992-09-01"), ymd("2020-12-31"))) |>
    inner_join(fishing_sample |> select(.tid, gid) |> distinct(.tid, .keep_all = TRUE)) |>
    left_join(lods_londun_distinct) |>
    mutate(has.lid = !is.na(.lid),
           D2 = floor_date(D2, "month")) |>
    count(D2, has.lid) |>
    ggplot(aes(D2, n, fill = has.lid)) +
    geom_col(position = "fill")

}
