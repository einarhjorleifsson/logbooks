# Create standard summary tables
# Input:  data-raw/data-dump/afli/*.parquet
# Output: data/afli/trip.parquet, station.parquet, catch.parquet

# Seeding ----------------------------------------------------------------------
library(whack) # pak::pak("einarhjorleifsson/whack")
library(geo)
library(tidyverse)
library(nanoparquet)

SCHEMA <- "afli"
dictionary   <- read_parquet("data/dictionary.parquet") |> filter(schema == SCHEMA)
gear_mapping <- read_parquet("data/gear/gear_mapping.parquet")

# Base -------------------------------------------------------------------------
base <-
  read_parquet("data-raw/data-dump/afli/stofn.parquet") |>
  wk_translate(dictionary, "messy", "clean") |>
  # 40 records have NA date — drop them
  filter(!is.na(date)) |>
  mutate(lon1 = -geoconvert.1(x1 * 100),
         lat1 =  geoconvert.1(y1 * 100),
         lon2 = -geoconvert.1(x2 * 100),
         lat2 =  geoconvert.1(y2 * 100)) |>
  select(.sid, vid, gid, date,
         lon1, lat1, lon2, lat2, sq, ssq,
         z1, z2,
         T2, hid2, n_crew,
         everything()) |>
  group_by(vid, T2, hid2) |>
  mutate(.tid = min(.sid)) |>
  group_by(vid, .tid) |>
  mutate(T1   = min(date, na.rm = TRUE),
         hid1 = NA_integer_) |>
  ungroup() |>
  mutate(schema = SCHEMA)

# QC
base <- base |>
  mutate(sq  = case_when(between(sq,  1, 999) ~ sq,  .default = NA),
         ssq = case_when(between(ssq, 0,   4) ~ ssq, .default = NA),
         # some utter garbage
         lat2 = case_when(.sid < 0 & lon2 <= -50 ~ NA, .default = lat2),
         lon2 = case_when(.sid < 0 & lon2 <= -50 ~ NA, .default = lon2))

# Trip -------------------------------------------------------------------------
# NOTE: trip table is derived — no raw trip source in this schema.
#       T1 is the minimum fishing date within the trip.
trip <-
  base |>
  select(.tid, vid, T1, hid1, T2, hid2, n_crew, schema) |>
  distinct()

# Station base (columns carried into the effort blocks)
base <- base |>
  select(.tid, .sid, gid, date, lon1, lat1, lon2, lat2, sq, ssq, z1, z2, schema)

# Effort aux blocks ------------------------------------------------------------
# Each block reads an auxiliary parquet, inner-joins with base to get the
# gid/date it needs for effort computation, then selects only .sid + effort
# columns. All blocks are combined and left-joined onto base, so every station
# in stofn is retained (effort = NA where no aux record exists).

## mobile (toga) ---------------------------------------------------------------
mobile_aux <-
  read_parquet("data-raw/data-dump/afli/toga.parquet") |>
  wk_translate(dictionary, "messy", "clean") |>
  inner_join(base |> filter(gid %in% c(5, 6, 7, 8, 9, 14, 15, 26, 38, 40)) |>
               select(.sid, gid, date), by = ".sid") |>
  # cap towtime per gear
  mutate(towtime = case_when(
    gid %in% c(5, 26) & towtime > 60 *  4 ~ 60 *  4,
    gid == 6           & towtime > 60 * 12 ~ 60 * 12,
    gid == 7           & towtime > 60 * 30 ~ 60 * 30,
    gid == 9           & towtime > 60 * 12 ~ 60 * 12,   # needs checking
    gid == 14          & towtime > 60 * 16 ~ 60 * 16,
    gid == 15          & towtime > 60 * 20 ~ 60 * 20,
    gid == 38          & towtime > 60 *  2 ~ 60 *  2,   # needs checking
    gid == 40          & towtime > 60 * 12 ~ 60 * 12,
    .default = towtime)) |>
  mutate(
    effort = case_when(
      gid %in% c(6, 7, 8, 9, 14, 15, 38, 40) ~ towtime / 60,
      gid %in% c(5, 18, 26, 39)               ~ 1,
      TRUE                                     ~ NA_real_),
    effort_unit = case_when(
      gid %in% c(6, 7, 8, 9, 14, 15, 38, 40) ~ "hours towed",
      gid %in% c(5, 18, 26, 39)               ~ "setting",
      TRUE                                     ~ NA_character_)) |>
  # derive t1 from on-bottom clock (HHMM integer), t2 from tow duration
  mutate(on.bottom = case_when(on.bottom >= 100 ~ on.bottom, .default = NA),
         hh = as.integer(on.bottom %/% 100),
         mm = as.integer(on.bottom %% 100),
         t1 = as_datetime(date) + hours(hh) + minutes(mm),
         t2 = t1 + minutes(towtime)) |>
  mutate(gear_width = if_else(!is.na(sweeps), sweeps, plow_width)) |>
  select(.sid, t1, t2, effort, effort_unit, towtime, gear_width)

## static (lineha: longline, gillnet, handline) --------------------------------
static_aux <-
  read_parquet("data-raw/data-dump/afli/lineha.parquet") |>
  wk_translate(dictionary, "messy", "clean") |>
  inner_join(base |> filter(gid %in% c(1, 2, 3)) |> select(.sid, gid),
             by = ".sid") |>
  mutate(onglar = case_when(gid == 1 & onglar > 1800 ~ 1800, .default = onglar),
         bjod   = case_when(gid == 1 & bjod   >  100 ~  100, .default = bjod),
         dregin = case_when(gid == 2 & dregin >  200 ~  200, .default = dregin),
         naetur = case_when(gid == 2 & naetur >    7 ~    7, .default = naetur)) |>
  mutate(
    effort = case_when(
      gid == 1 ~ as.numeric(onglar * bjod),
      gid == 2 ~ as.numeric(dregin * naetur),
      gid == 3 ~ as.numeric(faeri  * hours)),
    effort_unit = case_when(
      gid == 1 ~ "hooks",
      gid == 2 ~ "netnights",
      gid == 3 ~ "hookhours")) |>
  select(.sid, t0, t1, t2, effort, effort_unit)

## traps (gildra) --------------------------------------------------------------
trap_aux <-
  read_parquet("data-raw/data-dump/afli/gildra.parquet") |>
  wk_translate(dictionary, "messy", "clean") |>
  inner_join(base |> filter(gid %in% c(18, 39)) |> select(.sid, gid),
             by = ".sid") |>
  mutate(n_units = case_when(
    gid == 18 & n_units > 1500 ~ 1500,
    gid == 39 & n_units > 2000 ~ 2000,
    .default = n_units),
    hours = case_when(
    gid == 18 & hours > 260 ~ 260,
    gid == 39 & hours > 500 ~ 500,
    .default = hours)) |>
  mutate(effort = n_units * hours, effort_unit = "traphours") |>
  select(.sid, effort, effort_unit)

## seine / ring net (hringn) ---------------------------------------------------
seine_aux <-
  read_parquet("data-raw/data-dump/afli/hringn.parquet") |>
  wk_translate(dictionary, "messy", "clean") |>
  inner_join(base |> filter(gid %in% c(10, 12)) |> select(.sid, date),
             by = ".sid") |>
  mutate(effort = 1, effort_unit = "setting") |>
  # derive t1 from klukkan (HHMM integer)
  mutate(klukkan = case_when(klukkan >= 100 ~ klukkan, .default = NA),
         hh = as.integer(klukkan %/% 100),
         mm = as.integer(klukkan %% 100),
         t1 = as_datetime(date) + hours(hh) + minutes(mm)) |>
  select(.sid, t1, effort, effort_unit)

# Station ----------------------------------------------------------------------
station <-
  base |>
  left_join(bind_rows(mobile_aux, static_aux, trap_aux, seine_aux), by = ".sid") |>
  select(.tid, .sid, gid, date, t0, t1, t2,
         lon1, lat1, lon2, lat2, sq, ssq, z1, z2,
         effort, effort_unit, towtime, gear_width, schema) |>
  arrange(date, .sid, t0, t1, t2)

# Catch ------------------------------------------------------------------------
catch <-
  read_parquet("data-raw/data-dump/afli/afli.parquet") |>
  wk_translate(dictionary, "messy", "clean") |>
  select(.sid, sid, catch) |>
  group_by(.sid, sid) |>
  summarise(catch = sum(catch, na.rm = TRUE), .groups = "drop") |>
  # retain only catch records linked to a known station
  inner_join(station |> select(.sid, schema), by = ".sid") |>
  arrange(.sid, sid)

# Add "new" gear codes ---------------------------------------------------------
station <- station |>
  rename(gid_old = gid) |>
  left_join(gear_mapping |>
              filter(version == "old") |>
              select(gid_old = gid, gid = map),
            by = "gid_old") |>
  select(.tid, .sid, gid, gid_old, everything())

# Export -----------------------------------------------------------------------
trip    |> write_parquet("data/afli/trip.parquet")
station |> write_parquet("data/afli/station.parquet")
catch   |> write_parquet("data/afli/catch.parquet")
