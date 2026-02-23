# put nuisance variables (.sid, .tid, ...) last
library(tidyverse)
library(duckdbfs)
con <- duckdbfs::cached_connection()

# Functions --------------------------------------------------------------------
source("R/translate_name.R")
# Create the macro once
register_duckdb_function <- function() {
  con <- duckdbfs::cached_connection()
  DBI::dbExecute(con, "
    CREATE OR REPLACE MACRO rb_convert_DMdM(x) AS (
      SIGN(x) * (ABS(x) + (200.0/3.0) * ((ABS(x)/100.0) - TRUNC(ABS(x)/10000.0) * 100.0)) / 10000.0
    );
  ")
}

# seeding ----------------------------------------------------------------------
register_duckdb_function()
dictionary <- nanoparquet::read_parquet("data/dictionary.parquet")

# trip -------------------------------------------------------------------------
trip <-
  open_dataset("data-raw/data-dump/logbooks-fsafladagbok/ws_veidiferd.parquet") |>
  translate_names(dictionary) |>
  rename(.tid = id) |>
  select(vid, D1, hid1, D2, hid2, .tid, source = uppruni)
trip |> glimpse()
# base -------------------------------------------------------------------------
base <-
  duckdbfs::open_dataset("data-raw/data-dump/logbooks-fsafladagbok/ws_veidi.parquet") |>
  translate_names(dictionary) |>
  rename(.sid = id) |>
  mutate(date = as_date(t1)) |>
  # include vessel id in base?
  left_join(trip |> select(.tid, vid, source)) |>
  select(vid, gid, date, t1, t12, t2, x1, y1, x2, y2, z1, z2, .sid, .tid, source)
# convertion to lon and lat
#   Seems like source TACTICAL returns the last 2 digits as seconds, not decimal minutes
test <-
  base |>
  mutate(nchar = nchar(as.character(abs(x1))))
test |>
  count(source, nchar) |>
  collect() |>
  spread(nchar, n) |>
  knitr::kable()
test |>
  mutate(lon = case_when(source == "TRACKWELL" ~ -rb_convert_DMdm(x1 * 100),
                         .default = NA),
         lat = case_when(source == "TRACKWELL" ~ rb_convert_DMdm(y1 * 100),
                         .default = NA)) |>
  select(lon, lat) |>
  collect() |>
  drop_na() |>
  ramb::rb_mapdeck()
test |>
  filter(source == "Trackwell raun") |>
  select(lon = x1, lat = y1) |>
  collect() |>
  drop_na() |>
  mutate(lon = -geo::geoconvert.1(lon),
         lat = geo::geoconvert.1(lat)) |>
  ramb::rb_mapdeck()



base |> glimpse()
base |>
  mutate(n_char = nchar(as.character(abs(x1)))) |>
  filter(n_char == 6) |>
  left_join(trip |> select(.tid, source)) |>
  collect() |>
  select(x1, source) |>
  mutate(x11 = abs(x1)) |>
  separate(x11, into = c("dd", "mm", "ss"), sep = c(2, 4), remove = FALSE) |>
  count(ss, source) |>
  filter(source == "FRA TAKTIKAL") |>
  ggplot(aes(ss, n)) + geom_point()

|>
  count(ss) |>
  knitr::kable()
  select(lon = lon1, lat = lat1, speed = gid) |>
  collect() |>
  ramb::rb_mapdeck()
  count(n_char) |>
  mutate(p = n / sum(n) * 100) |>
  collect() |>
  arrange(-p) |>
  knitr::kable(digits = 1)
base |>
  mutate(n_char = nchar(as.character(abs(lon2)))) |>
  count(source, n_char) |>
  group_by(source) |>
  mutate(p = n / sum(n) * 100) |>
  ungroup() |>
  collect() |>
  arrange(source, -p) |>
  knitr::kable(digits = 1)
# Need to fix the above before proceeding
base <-
  base |>
  # 40 records get dropped
  dplyr::filter(!is.na(date)) |>
  dplyr::mutate(lon1 = -rb_convert_DMdm(lon1 * 100),
                lat1 =  rb_convert_DMdm(lat1 * 100),
                lon2 = -rb_convert_DMdm(lon2 * 100),
                lat2 =  rb_convert_DMdm(lat2 * 100)) |>
  # some utter garbage - there may be a little more
  dplyr::mutate(lat2 = case_when(.sid < 0 & lon2 <= -50 ~ NA,
                                 .default = lat2),
                lon2 = case_when(.sid < 0 & lon2 <= -50 ~ NA,
                                 .default = lon2)) |>
  dplyr::select(.sid, vid, gid, date,
                t1, t12, t2,
                # geographic location
                lon1, lat1, lon2, lat2,
                # depth
                z1, z2,
                .tid,
                dplyr::everything())

# mobile -----------------------------------------------------------------------
mobile <-
  base |>
  dplyr::select(.sid:.tid) |>
  dplyr::filter(gid %in% c(6, 7, 8, 9, 14, 15, 38, 40, 5, 26)) |>
  dplyr::inner_join(open_dataset("data-raw/data-dump/logbooks-old/afli_toga.parquet") |>
                      translate_names(dictionary),
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
  select(-c(hh, mm)) |>
  dplyr::select(.sid:.tid,
                towtime,                     # in minutes
                effort,
                effort_unit,
                # mesh,
                # mesh_min,
                doors,              # in kilograms
                headline,
                sweeps,             # in meters ???
                plow_width)
                #tempb1,             # bottom temperature
                #tempb2,
                #temps1,             # surface temperature
                #temps2)
# static -----------------------------------------------------------------------
static <-
  base |>
  dplyr::select(.sid:n_crew) |>
  dplyr::filter(gid %in% c(1, 2, 3)) |>
  dplyr::inner_join(open_dataset("data-raw/data-dump/logbooks-old/afli_lineha.parquet") |>
                      translate_names(dictionary),
                    by = ".sid") |>
  # cap effort
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
  dplyr::select(.sid:date,
                t0, t1, t2,
                lon1:n_crew,
                effort,
                effort_unit)
                #mesh,
                #tempb1,             # bottom temperature
                #temps1)             # surface temperature
# Move to QC -------------------------------------------------------------------
static |>
  mutate(has.t0 = ifelse(!is.na(t0), "yes", "no"),
         has.t1 = ifelse(!is.na(t1), "yes", "no"),
         has.t2 = ifelse(!is.na(t2), "yes", "no")) |>
  mutate(year = year(date)) |>
  count(year, gid, has.t0, has.t1, has.t2) |>
  filter(year >= 1985) |>
  collect() |>
  gather(var, val, has.t0:has.t2) |>
  ggplot(aes(year, n, fill = val)) +
  geom_col() +
  facet_grid(var ~ gid, scales = "free_y") +
  scale_fill_brewer(palette = "Set1")
# traps ------------------------------------------------------------------------
traps <-
  base |>
  # Loose quite some records
  filter(gid %in% c(18, 39)) |>
  inner_join(open_dataset("data-raw/data-dump/logbooks-old/afli_gildra.parquet") |>
               translate_names(dictionary)) |>
  mutate(n_units = case_when(gid == 18 & n_units > 1500 ~ 1500,
                             gid == 39 & n_units > 2000 ~ 2000,
                             .default = n_units),
         hours = case_when(gid == 18 & hours > 260 ~ 260,
                           gid == 39 & hours > 500 ~ 500,
                           .default = hours)) |>
  dplyr::mutate(effort = n_units * hours,
                effort_unit = "traphours") |>
  select(.sid:n_crew, effort, effort_unit)
# seine ------------------------------------------------------------------------
seine <-
  base |>
  filter(gid %in% c(10, 12)) |>
  inner_join(open_dataset("data-raw/data-dump/logbooks-old/afli_hringn.parquet") |>
               translate_names(dictionary)) |>
  dplyr::mutate(effort = 1, effort_unit = "setting") |>
  dplyr::mutate(klukkan = dplyr::if_else(klukkan < 100, NA, klukkan, NA),
                hh = as.integer(klukkan%/%100),
                mm = as.integer(klukkan%%100),
                t1 = as_datetime(date) + hours(hh) + minutes(mm),
                .after = date) |>
  select(-c(hh, mm)) |>
  select(.sid:n_crew, effort, effort_unit)

lb_old <-
  # TODO: Check order of things, ... and mesh
  bind_rows(static |> collect(),
            mobile |> collect(),
            seine  |> collect(),
            traps  |> collect()) |>
  arrange(date, vid, t0, t1, t2) |>
  mutate(base = "old")
# catch ------------------------------------------------------------------------
catch <-
  open_dataset("data-raw/data-dump/logbooks-old/afli_afli.parquet") |>
  translate_names(dictionary) |>
  select(.sid, sid, catch) |>
  group_by(.sid, sid) |>
  summarise(catch = sum(catch, na.rm = TRUE),
            .groups = "drop") |>
  collect() |>
  # ensure that catch record not an orphan
  inner_join(lb_old |> select(.sid)) |>
  arrange(.sid, sid) |>
  mutate(base = "old")

# get landings gear ------------------------------------------------------------
#  approch
#  bind landings and logbook data (vessel, date and gear)
#  for each vessel arrange in descending date order and source (landings before
#  logbooks)
#  use fill down for the gear in landings assigning landings gear to each
#  logbook record
landings <-
  open_dataset("/heima/einarhj/stasi/fishydata/data/landings/agf_stations.parquet") |>
  collect()
# this seems to be the way
gid_match <-
  lb_old |>
  filter(date >= ymd("2007-09-01")) |>
  select(.sid, vid, date, gid) |>
  mutate(source = "logbook") |>
  bind_rows(landings |>
              select(date, vid, agf_gid = gid) |>
              distinct() |>
              mutate(source = "landings")) |>
  arrange(vid, desc(date), source) |>
  group_by(vid) |>
  fill(agf_gid, .direction = "down") |>
  ungroup()

lb_old <-
  lb_old |>
  left_join(gid_match |>
              select(.sid, vid, agf_gid)) |>
  select(.sid, gid, agf_gid, everything())

# save -------------------------------------------------------------------------
lb_old |> nanoparquet::write_parquet("data/base-old.parquet")
catch  |> nanoparquet::write_parquet("data/catch-old.parquet")

# QC
lb_old |>
  filter(date >= ymd("2007-09-01")) |>
  count(gid, agf_gid) |>
  arrange(-n) |>
  mutate(p = n / sum(n) * 100,
         cp = cumsum(p)) |>
  knitr::kable(digits = 1)
