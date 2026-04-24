library(tidyverse)
library(nanoparquet)

# 2 New logbooks ---------------------------------------------------------------
# The new logbooks are in principle a total mess that need to be fixed upstream
#  Following is thus just an interrim hack. The function call to the new data
#  are a little different since it is still in development.

## Functions -------------------------------------------------------------------
# should possible move functions to the omar-package
lb_trip_new <- function() {

  read_parquet("data-raw/data-dump/adb/trip_v.parquet") |>
    select(trip_id,
           vid = vessel_no,
           T1 = departure,
           hid1 = departure_port_no,
           T2 = landing,
           hid2 = landing_port_no,
           source)
}
lb_station_new0 <- function() {
  read_parquet("data-raw/data-dump/adb/station_v.parquet") |>
    select(trip_id,
           station_id,
           gid = gear_no,
           t1 = fishing_start,
           t2 = fishing_end,
           lon = longitude,
           lat = latitude,
           lon2 = longitude_end,
           lat2 = latitude_end,
           z1 = depth,
           z2 = depth_end,
           tow_start,
           everything())
}
lb_base_new <- function(con) {
  lb_trip_new() |>
    inner_join(lb_station_new0() |>
                 select(trip_id:tow_start),
               by = "trip_id") |>
    select(vid, gid, t1:tow_start, everything()) |>
    mutate(whack = case_when(between(lon, 10, 30) & between(lat, 62.5, 67.6) ~ "mirror",
                             between(lon, -3, 3) & gid != 7 ~ "ghost",
                             .default = "ok"),
           lon = ifelse(whack == "mirror",
                        -lon,
                        lon),
           lon2 = ifelse(whack == "mirror",
                         -lon2,
                         lon2))
}
lb_catch_new <- function() {
  # why not catch_v.parquet
  read_parquet("data-raw/data-dump/adb/catch.parquet") |>
  mutate(catch = case_when(condition == "GUTT" ~ quantity / 0.8,
                           condition == "UNGU" ~ quantity,
                           .default = NA)) |>
    select(station_id = fishing_station_id,
           sid = species_no,
           catch,
           weight,
           quantity,
           condition,
           catch_type = source_type)
}


## Only records not in old logbooks --------------------------------------------
BASE_new <-
  lb_base_new() |>
  # filter(year(t1) %in% YEARS) |>
  # collect(n = Inf) |>
  select(vid:z2, trip_id, datel = T2, source:whack) |>
  mutate(date = as_date(t1),
         datel = as_date(datel),
         base = "new")
if(FALSE) {
  BASE_new_n0 <- nrow(BASE_new)
  # only data where the date fishing and vessels are not already in the old
  #  logbooks. This reduces the number of records from ~212 thousand to
  #  ~88 thousand
  BASE_new <-
    BASE_new |>
    left_join(LGS_old |>
                select(vid, date) |>
                distinct() |>
                mutate(in.old = "yes"),
              #multiple = "all",
              by = join_by(vid, date)) |>
    mutate(in.old = replace_na(in.old, "no"))

  BASE_new |>
    count(source, in.old) |>
    spread(in.old, n) |>
    knitr::kable(caption = "Number of records in new database that are also in the old database.")
}

## Checks ----------------------------------------------------------------------
### Should one remove whacks?? - not if using positions from ais ---------------
#   mirror: record where lon is positive but should be negative
#   ghost: records around the meridian
BASE_new |>
  count(source, whack) |>
  spread(whack, n)
### Any abberrant trend in the number of sets by month? ------------------------
if(FALSE) {
  bind_rows(
    LGS_old |>   select(gid, date, base),
    BASE_new  |>
      filter(in.old == "no") |>
      select(gid, date, base)) |>
    left_join(GEARS_trim |> mutate(gclass = paste(str_pad(gid, 2, pad = "0"), veidarfaeri)) |> select(gid, gclass)) |>
    mutate(date = floor_date(date, "month")) |>
    count(date, gclass) |>
    filter(year(date) %in% 2018:2022) |>
    ggplot(aes(date, n)) +
    geom_point(size = 0.5) +
    facet_wrap(~ gclass, scales = "free_y")
}


## Mobile gear -----------------------------------------------------------------
MOBILE_new <-
  BASE_new |>
  inner_join(
    read_parquet("data-raw/data-dump/adb/trawl_and_seine_net_v.parquet"),
    by = join_by(station_id)
  ) |>
  mutate(effort = case_when(gid %in% c(6, 7) ~ as.numeric(difftime(t2, t1, units = "hours")),
                            gid %in% 5 ~ 1,
                            .default = NA),
         effort_unit = case_when(gid %in% c(6, 7) ~ "hours towed",
                                 gid %in% 5  ~  "setting",
                                 .default = NA)) |>
  rename(sweeps = bridle_length) |>
  select(station_id, sweeps, effort, effort_unit)
## Static gear -----------------------------------------------------------------
STATIC_new <-
  BASE_new |>
  inner_join(
    read_parquet("data-raw/data-dump/adb/line_and_net_v.parquet"),
    by = join_by(station_id)
  ) |>
  mutate(dt = as.numeric(difftime(t2, t1, unit = "hours")),
         effort = case_when(gid == 3 ~ dt,
                            gid %in% c(2, 11, 25, 29, 91, 92) ~ dt/24 * nets,
                            gid %in% 1 ~ hooks,
                            .default = NA),
         effort_unit = case_when(gid == 3 ~ "hookhours",
                                 gid %in% c(2, 11, 25, 29, 91, 92) ~ "netnights",
                                 gid %in% 1 ~ "hooks",
                                 .default = NA)) |>
  select(station_id, effort, effort_unit)
## Dredge gear -----------------------------------------------------------------
DREDGE_new <-
  BASE_new |>
  inner_join(
    read_parquet("data-raw/data-dump/adb/dredge_v.parquet"),
    by = join_by(station_id)
  ) |>
  mutate(effort = as.numeric(difftime(t2, t1, units = "hours")),
         effort_unit = "hours towed",
         plow_width = 2) |>
  select(station_id, plow_width, effort, effort_unit)
## Trap gear -------------------------------------------------------------------
TRAP_new <-
  BASE_new |>
  inner_join(
    read_parquet("data-raw/data-dump/adb/trap_v.parquet"),
    by = join_by(station_id)
  ) |>
  mutate(dt = as.numeric(difftime(t2, t1, units = "hours")),
         effort = dt * number_of_traps,
         effort_unit = "trap hours") |>
  select(station_id, effort, effort_unit)
## Seine gear ------------------------------------------------------------------
SEINE_new <-
  BASE_new |>
  inner_join(
    read_parquet("data-raw/data-dump/adb/surrounding_net_v.parquet"),
    by = join_by(station_id)
  ) |>
  mutate(effort = 1,
         effort_unit = "setting") |>
  select(station_id, effort, effort_unit)

BASE_new_aux <-
  bind_rows(MOBILE_new,
            STATIC_new,
            DREDGE_new,
            TRAP_new,
            SEINE_new)

## Check -----------------------------------------------------------------------
### Orphan effort files --------------------------------------------------------
n1 <- nrow(BASE_new)
n2 <- nrow(BASE_new_aux)
print(paste0("Records in base: ", n1, " Records in auxillary: ", n2))
BASE_new |>
  mutate(orphan = ifelse(station_id %in% BASE_new_aux$station_id, "no", "yes")) |>
  count(source, orphan) |>
  spread(orphan, n) |>
  knitr::kable(caption = "Source of effort orphan files")
BASE_new |>
  mutate(orphan = ifelse(station_id %in% BASE_new_aux$station_id, "no", "yes")) |>
  filter(orphan == "yes") |>
  count(source, gid) |>
  spread(gid, n) |>
  knitr::kable(caption = "Gear list of effort orphan files")

## Combine the (new) logbooks --------------------------------------------------
LGS_new <-
  BASE_new |>
  left_join(BASE_new_aux,
            by = join_by(station_id)) |>
  select(.sid = station_id, vid, gid, date, t1, t2, lon, lat, lon2, lat2, z1, z2,
         datel, effort, effort_unit, sweeps, plow_width, base)

## Catch -----------------------------------------------------------------------
CATCH_new <-
  lb_catch_new() |>
  collect(n = Inf) |>
  inner_join(BASE_new |> select(station_id),
             by = join_by(station_id)) |>
  # filter(station_id %in% BASE_new$station_id) |>
  select(.sid = station_id, sid, catch)

LGS_new   |> write_parquet("data/adb/station_new-brute-force.parquet")
CATCH_new |> write_parquet("data/adb/catch_new-brute-force.parquet")

# sanity test ------------------------------------------------------------------
## afli vs adb
afli <- read_parquet("data/afli/station.parquet") |>
  mutate(year = year(date)) |>
  count(year)

bind_rows(brute |> mutate(source = "brute"),
          afli  |> mutate(source = "afli")) |>
  filter(between(year, 2008, 2026)) |>
  ggplot(aes(year, n, colour = source)) +
  geom_point()
## brute-force-vs-scripts/01_adb_convert.R


brute <- read_parquet("data/adb/station_new-brute-force.parquet") |>
  mutate(year = year(date)) |>
  count(year)
adb <- read_parquet("data/adb/station.parquet") |>
  mutate(year = year(date)) |>
  count(year)
bind_rows(brute |> mutate(source = "brute"),
          adb   |> mutate(source = "adb")) |>
  filter(between(year, 2008, 2026)) |>
  ggplot(aes(year, n, colour = source)) +
  geom_point()

bind_rows(brute |> mutate(source = "brute"),
          adb   |> mutate(source = "adb")) |>
  filter(between(year, 2020, 2025)) |>
  spread(source, n, fill = 0) |>
  mutate(diff = brute - adb)

