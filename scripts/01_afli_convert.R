# Create standard summary tables
# Input: data-raw/data-dump/afli/*.parquet
# Outpt: data/afli/*.parquet

library(whack) # pak::pak("einarhjorleifsson/whack)
library(geo)
library(tidyverse)
library(nanoparquet)

# seeding ----------------------------------------------------------------------
SCHEMA <- "afli"
dictionary <- read_parquet("data/dictionary.parquet") |>
  filter(schema == SCHEMA)

#' trip ------------------------------------------------------------------------
#' NOTE: No trip table in this schema - is derived from base below

# base -------------------------------------------------------------------------
base <-
  read_parquet("data-raw/data-dump/afli/stofn.parquet") |>
  wk_translate(dictionary, "name", "new") |>
  # 40 records get dropped
  dplyr::filter(!is.na(date)) |>
  dplyr::mutate(lon1 = -geoconvert.1(x1 * 100),
                lat1 =  geoconvert.1(y1 * 100),
                lon2 = -geoconvert.1(x2 * 100),
                lat2 =  geoconvert.1(y2 * 100)) |>
  dplyr::select(.sid, vid, gid, date,
                # geographic location
                lon1, lat1, lon2, lat2, sq, ssq,
                # depth
                z1, z2,
                # environmental parameters
                # winddirection, beaufort, m_sec,
                # landings statistics
                T2, hid2, n_crew,
                dplyr::everything()) |>
  group_by(vid, T2, hid2) |>
  mutate(.tid = min(.sid)) |>
  group_by(vid, .tid) |>
  # should one have a comment here, fake data
  mutate(T1 = min(date, na.rm = TRUE), # maximum departure date
         hid1 = NA_integer_) |>
  ungroup() |>
  mutate(schema = schema)

# QC
base |>
  mutate(sq = case_when(between(sq, 1, 999) ~ sq,
                               .default = NA),
                ssq = case_when(between(ssq, 0, 4) ~ ssq,
                                .default = NA),
         # some utter garbage - there may be a little more
         lat2 = case_when(.sid < 0 & lon2 <= -50 ~ NA,
                                 .default = lat2),
                lon2 = case_when(.sid < 0 & lon2 <= -50 ~ NA,
                                 .default = lon2))
# trip derived -----------------------------------------------------------------
#' NOTE
#'  D! is derived from the first recorded fishing date
trip <-
  base |>
  group_by(.tid) |>
  select(.tid, vid, T1, hid1, T2, hid2, n_crew, schema) |>
  distinct()

# remains to pass downstream
base <-
  base |>
  select(.tid, .sid, gid:z2, schema)

# mobile -----------------------------------------------------------------------
mobile <-
  base |>
  dplyr::filter(gid %in% c(6, 7, 8, 9, 14, 15, 38, 40, 5, 26)) |>
  dplyr::inner_join(read_parquet("data-raw/data-dump/afli/toga.parquet") |>
                      wk_translate(dictionary, "name", "new"),
                    by = ".sid") |>
  # cap effort
  mutate(towtime = case_when(gid %in% c(5, 26) & towtime > 60 * 4 ~ 60 * 4,
                             gid == 6 & towtime > 60 * 12 ~ 60 * 12,
                             gid == 7 & towtime > 60 * 30 ~ 60 * 30,
                             # this needs checking
                             gid == 9 & towtime > 60 * 12 ~ 60 * 12,
                             gid == 14 & towtime > 60 * 16 ~ 60 * 16,
                             gid == 15 & towtime > 60 * 20 ~ 60 * 20,
                             # this needs checking
                             gid == 38 & towtime > 60 * 2 ~ 60 * 2,
                             gid == 40 & towtime > 60 * 12 ~ 60 * 12,
                             .default = towtime)) |>
  dplyr::mutate(effort = dplyr::case_when(gid %in% c(6, 7, 8, 9, 14, 15, 38, 40) ~ towtime / 60,
                                          # for seine and traps use setting as effort
                                          gid %in% c(5, 18, 26, 39) ~ 1,
                                          TRUE ~ NA_real_),
                effort_unit = dplyr::case_when(gid %in% c(6, 7, 8, 9, 14, 15, 38, 40) ~ "hours towed",
                                               # for seine just use the setting
                                               gid %in% c(5, 18, 26, 39) ~ "setting",
                                               TRUE ~ NA_character_)) |>
  dplyr::mutate(on.bottom = dplyr::if_else(on.bottom < 100, NA, on.bottom, NA),
                hh = as.integer(on.bottom%/%100),
                mm = as.integer(on.bottom%%100),
                t1 = as_datetime(date) + hours(hh) + minutes(mm),
                t2 = t1 + minutes(towtime),
                .after = date) |>
  dplyr::mutate(gear_width = if_else(!is.na(sweeps), sweeps, plow_width, NA)) |>
  dplyr::select(.tid:z2,
                effort, effort_unit,
                towtime,                     # in minutes, t1 and t2 could not be derived
                gear_width,
                schema) |>
  select(-c(hh, mm))

# static -----------------------------------------------------------------------
static <-
  base |>
  dplyr::filter(gid %in% c(1, 2, 3)) |>
  dplyr::inner_join(read_parquet("data-raw/data-dump/afli/lineha.parquet") |>
                      wk_translate(dictionary, "name", "new"),
                    by = ".sid") |>
  # QC cap effort - kind of have to have it here
  mutate(onglar = case_when(gid == 1 & onglar > 1800 ~ 1800,
                            .default = onglar),
         bjod = case_when(gid == 1 & bjod > 100 ~ 100,
                          .default = bjod),
         dregin = case_when(gid == 2 & dregin > 200 ~ 200,
                            .default = dregin),
         naetur = case_when(gid == 2 & naetur > 7 ~ 7,
                            .default = naetur)) |>
  dplyr::mutate(effort = dplyr::case_when(gid == 1 ~ as.numeric(onglar * bjod),
                                          gid == 2 ~ as.numeric(dregin * naetur),
                                          gid == 3 ~ as.numeric(faeri * hours)),
                effort_unit = dplyr::case_when(gid == 1 ~ "hooks",
                                               gid == 2 ~ "netnights",
                                               gid == 3 ~ "hookhours")) |>
  select(.tid:date,
         t0, t1, t2,
         lon1:z2,
         effort,
         effort_unit,
         schema)

# traps ------------------------------------------------------------------------
traps <-
  base |>
  # Loose quite some records
  filter(gid %in% c(18, 39)) |>
  inner_join(read_parquet("data-raw/data-dump/afli/gildra.parquet") |>
               wk_translate(dictionary, "name", "new")) |>
  # QC
  mutate(n_units = case_when(gid == 18 & n_units > 1500 ~ 1500,
                             gid == 39 & n_units > 2000 ~ 2000,
                             .default = n_units),
         hours = case_when(gid == 18 & hours > 260 ~ 260,
                           gid == 39 & hours > 500 ~ 500,
                           .default = hours)) |>
  dplyr::mutate(effort = n_units * hours,
                effort_unit = "traphours") |>
  select(.tid:z2, effort, effort_unit, schema)

# seine ------------------------------------------------------------------------
seine <-
  base |>
  filter(gid %in% c(10, 12)) |>
  inner_join(read_parquet("data-raw/data-dump/afli/hringn.parquet") |>
               wk_translate(dictionary, "name", "new")) |>
  dplyr::mutate(effort = 1, effort_unit = "setting") |>
  dplyr::mutate(klukkan = dplyr::if_else(klukkan < 100, NA, klukkan, NA),
                hh = as.integer(klukkan%/%100),
                mm = as.integer(klukkan%%100),
                t1 = as_datetime(date) + hours(hh) + minutes(mm),
                .after = date) |>
  select(-c(hh, mm)) |>
  select(.tid:z2, effort, effort_unit, schema)

# station table ----------------------------------------------------------------
station <-
  # TODO: Check order of things, ... and mesh
  bind_rows(static,
            mobile,
            seine,
            traps) |>
  arrange(date, .sid, t0, t1,  t2)

# catch ------------------------------------------------------------------------
catch <-
  read_parquet("data-raw/data-dump/afli/afli.parquet") |>
  wk_translate(dictionary, "name", "new") |>
  select(.sid, sid, catch) |>
  group_by(.sid, sid) |>
  summarise(catch = sum(catch, na.rm = TRUE),
            .groups = "drop") |>
  # ensure that catch record not an orphan
  inner_join(station |> select(.sid, schema)) |>
  arrange(.sid, sid)


# save -------------------------------------------------------------------------
trip    |> write_parquet("data/afli/trip.parquet")
station |> write_parquet("data/afli/station.parquet")
catch   |> write_parquet("data/afli/catch.parquet")
