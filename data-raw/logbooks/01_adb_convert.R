# Create standard summary tables
# Input: data-dump/logbooks/adb/*.parquet
# Outpt: data-raw/logbooks/adb/*.parquet

library(whack) # pak::pak("einarhjorleifsson/whack)
library(geo)
library(tidyverse)
library(nanoparquet)

# seeding ----------------------------------------------------------------------
SCHEMA <- "adb"
dictionary <- read_parquet("data/dictionary.parquet") |>
  filter(schema == SCHEMA)

# trip -------------------------------------------------------------------------
trip <- read_parquet("data-dump/logbooks/adb/trip_v.parquet") |>
  wk_translate(dictionary) |>
  select(.tid, vid, T1, hid1, T2, hid2) |>
  mutate(schema = SCHEMA)

# base -------------------------------------------------------------------------
base <-
  read_parquet("data-dump/logbooks/adb/station_v.parquet") |>
  wk_translate(dictionary) |>
  mutate(date = as_date(coalesce(t1, t3, t4))) |>
  select(.tid, .sid, gid, date,
         t1, t3, t4,
         lon1, lat1, lon2, lat2,
         z1, z2)

# aux tables -------------------------------------------------------------------
## Each block computes effort for its gear class and selects only
##   .sid, effort, effort_unit, and any gear-specific columns (towtime,
##   gear_width).  All inner_joins are on .sid only; base carries gid and
##   times needed for the calculations.

## mobile (trawl / seine-net) --------------------------------------------------
mobile_aux <-
  read_parquet("data-dump/logbooks/adb/trawl_and_seine_net_v.parquet") |>
  wk_translate(dictionary) |>                  # station_id → .sid; bridle_length → sweeps
  inner_join(base |> select(.sid, gid, t3, t4), by = ".sid") |>
  mutate(
    towtime = as.numeric(difftime(t4, t3, units = "mins")),
    effort = case_when(
      gid %in% c(6, 7, 14) ~ towtime / 60,
      gid == 5              ~ 1,
      .default = NA_real_
    ),
    effort_unit = case_when(
      gid %in% c(6, 7, 14) ~ "hours towed",
      gid == 5              ~ "setting",
      .default = NA_character_
    ),
    gear_width = if_else(!is.na(sweeps), sweeps, NA_real_)
  ) |>
  select(.sid, effort, effort_unit, towtime, gear_width)

## dredge (plógur) -------------------------------------------------------------
##   dredge_v covers gid 5, 6, 7, 51, 53 that are absent from trawl_and_seine_net_v
dredge_aux <-
  read_parquet("data-dump/logbooks/adb/dredge_v.parquet") |>
  wk_translate(dictionary) |>
  inner_join(base |> select(.sid, t3, t4), by = ".sid") |>
  mutate(
    towtime   = as.numeric(difftime(t4, t3, units = "mins")),
    effort    = towtime / 60,
    effort_unit = "hours towed",
    gear_width  = width
  ) |>
  select(.sid, effort, effort_unit, towtime, gear_width)

## static (longline / gillnet / handline) --------------------------------------
static_aux <-
  read_parquet("data-dump/logbooks/adb/line_and_net_v.parquet") |>
  wk_translate(dictionary) |>
  inner_join(base |> select(.sid, gid, t3, t4), by = ".sid") |>
  mutate(
    dt = as.numeric(difftime(t4, t3, units = "hours")),
    effort = case_when(
      gid == 1                             ~ hooks,
      gid %in% c(2, 11, 25, 29, 91, 92)   ~ nets * dt / 24,
      gid == 3                             ~ dt,
      .default = NA_real_
    ),
    effort_unit = case_when(
      gid == 1                             ~ "hooks",
      gid %in% c(2, 11, 25, 29, 91, 92)   ~ "netnights",
      gid == 3                             ~ "hookhours",
      .default = NA_character_
    )
  ) |>
  select(.sid, effort, effort_unit)

## traps -----------------------------------------------------------------------
trap_aux <-
  read_parquet("data-dump/logbooks/adb/trap_v.parquet") |>
  wk_translate(dictionary) |>
  inner_join(base |> select(.sid, t3, t4), by = ".sid") |>
  mutate(
    dt          = as.numeric(difftime(t4, t3, units = "hours")),
    effort      = number_of_traps * dt,
    effort_unit = "traphours"
  ) |>
  select(.sid, effort, effort_unit)

## seine / surrounding net -----------------------------------------------------
seine_aux <-
  read_parquet("data-dump/logbooks/adb/surrounding_net_v.parquet") |>
  wk_translate(dictionary) |>
  inner_join(base |> select(.sid), by = ".sid") |>
  mutate(effort = 1, effort_unit = "setting") |>
  select(.sid, effort, effort_unit)

# station table ----------------------------------------------------------------
station <-
  base |>
  left_join(
    bind_rows(mobile_aux, dredge_aux, static_aux, trap_aux, seine_aux),
    by = ".sid"
  ) |>
  mutate(schema = SCHEMA) |>
  arrange(date, .sid, t1, t3, t4)

# catch ------------------------------------------------------------------------
catch <-
  read_parquet("data-dump/logbooks/adb/catch.parquet") |>
  wk_translate(dictionary) |>
  rename(.sid = fishing_station_id,
         sid = species_no,
         catch = weight) |>
  select(.sid, sid, catch) |>
  group_by(.sid, sid) |>
  summarise(catch = sum(catch, na.rm = TRUE),
            .groups = "drop") |>
  # ensure that catch record not an orphan
  inner_join(station |> select(.sid, schema)) |>
  arrange(.sid, sid)

# save -------------------------------------------------------------------------
trip    |> write_parquet("data-raw/logbooks/adb/trip.parquet")
station |> rename(gid_old = gid) |> write_parquet("data-raw/logbooks/adb/station.parquet")
catch   |> write_parquet("data-raw/logbooks/adb/catch.parquet")
