# Match merged logbook trips to landing register records.
# Builds a .tid → .tid_land crosswalk and writes it to lid_map.parquet.
# Also writes .tid_land and match_type back into trip.parquet.
#
# Input:  data/logbooks/{trip,station,fishing_sample,catch}.parquet
#         data/landings/{landings,catch}.parquet
#         data/gear/gear_mapping.parquet
# Output: data/logbooks/lid_map.parquet  — columns: .tid, schema, .tid_land, match_type
#         data/logbooks/trip.parquet     — extended with .tid_land, match_type
#
# Eight-step cascade in decreasing key specificity.
# Each step operates only on trips/landings not consumed by earlier steps and
# accepts only unambiguous matches (exactly one .tid_land candidate per .tid).
#
#   Step 1 — exact (vid, date, hid)
#   Step 2 — (vid, date±1, hid): overnight trips / midnight-straddle
#   Step 3 — (vid, date): port dropped, same day
#   Step 4 — (vid, date±1): port dropped, one-day relaxation
#   Step 5 — (vid, date±2): systematic 2-day recording lag (~3,400 matches, 11:1 ratio)
#   Step 6 — demersal weight match: log-ratio ≤ WEIGHT_TOL_DEMERSAL, gap ≥ WEIGHT_MIN_GAP
#   Step 7 — pelagic weight match:  log-ratio ≤ WEIGHT_TOL_PELAGIC  (expect ~0 matches)
#   Step 8 — expected-date match: trips with recorded date > DATE_ERR_THRESHOLD days
#             from .tid-interpolated expected date; primarily a QA diagnostic

library(duckdbfs)
library(tidyverse)

# Parameters -------------------------------------------------------------------
WEIGHT_TOL_DEMERSAL <- log(1.3)   # accept if within ~30% of landing weight
WEIGHT_TOL_PELAGIC  <- log(2.0)   # accept if within ~100% (catch transfers)
WEIGHT_MIN_GAP      <- log(1.5)   # best must be ≥ 1.5× closer than runner-up
DATE_ERR_THRESHOLD  <- 35L        # days; larger deviation flags likely wrong-date entry
DATE_ERR_WINDOW     <- 3L         # ± days around expected date to search

## Landing register -------------------------------------------------------------
# One row per (vid × date × hid); catch summed across species.
# Multiple .lids on the same key are collapsed to min(.lid) = .tid.
land <-
  duckdbfs::open_dataset("data/landings/landings.parquet") |> collect() |>
  filter(vid > 0, date >= ymd("2007-09-01"), date <= ymd("2025-12-12")) |>
  left_join(
    duckdbfs::open_dataset("data/landings/catch.parquet") |> collect() |>
      group_by(.tid) |>
      summarise(catch = sum(catch), .groups = "drop"),
    by = ".tid"
  ) |>
  rename(.tid_land = .tid)

## Logbook trips with total catch -----------------------------------------------
trip <-
  duckdbfs::open_dataset("data/logbooks/trip.parquet") |> collect() |>
  mutate(date = as_date(T2), hid = hid2) |>
  left_join(
    duckdbfs::open_dataset("data/logbooks/catch.parquet") |> collect() |>
      left_join(
        duckdbfs::open_dataset("data/logbooks/station.parquet") |> collect() |> select(.sid, .tid, schema),
        by = ".sid"
      ) |>
      filter(!is.na(.tid)) |>
      group_by(.tid) |>
      summarise(catch = sum(as.numeric(catch), na.rm = TRUE), .groups = "drop"),
    by = ".tid"
  ) |>
  filter(catch > 0) |>
  select(.tid, vid, date, hid, port, catch, schema)

## Cascade helpers --------------------------------------------------------------
try_match <- function(unmatched, land_tbl, by, type) {
  unmatched |>
    inner_join(land_tbl, by = by) |>
    group_by(.tid) |>
    filter(n_distinct(.tid_land) == 1) |>
    slice(1) |>
    ungroup() |>
    mutate(match_type = type)
}

try_match_weight <- function(unmatched, land_tbl, consumed_land, tol, gap, type) {
  unmatched |>
    inner_join(
      land_tbl |> filter(!.tid_land %in% consumed_land),
      by = c("vid", "date")
    ) |>
    mutate(score = abs(log(catch / catch_land))) |>
    filter(is.finite(score), score <= tol) |>
    group_by(.tid) |>
    arrange(score, .by_group = TRUE) |>
    filter(n() == 1 | (score[2] - score[1]) >= gap) |>
    slice(1) |>
    ungroup() |>
    mutate(match_type = type)
}

## Steps 1–5: key-based matching -----------------------------------------------
# Step 1: exact key
m1 <- try_match(trip, land, c("vid", "date", "hid"), "01 exact")

# Shifted land tables reused across steps 2, 4, and weight matching
land_pm1 <- bind_rows(
  land |> mutate(date = date - 1),
  land |> mutate(date = date + 1)
)
land_pm2 <- bind_rows(
  land |> mutate(date = date - 2),
  land |> mutate(date = date + 2)
)

# Step 2: date ±1 day, port still required
m2 <- try_match(
  trip     |> filter(!.tid      %in% m1$.tid),
  land_pm1 |> filter(!.tid_land %in% m1$.tid_land),
  c("vid", "date", "hid"),
  "02 exact ±1 day"
)

# Step 3: same day, port dropped
m3 <- try_match(
  trip |> filter(!.tid      %in% c(m1$.tid,      m2$.tid)),
  land |> filter(!.tid_land %in% c(m1$.tid_land, m2$.tid_land)),
  c("vid", "date"),
  "03 vid_date"
)

# Step 4: ±1 day, port dropped
m4 <- try_match(
  trip     |> filter(!.tid      %in% c(m1$.tid,      m2$.tid,      m3$.tid)),
  land_pm1 |> filter(!.tid_land %in% c(m1$.tid_land, m2$.tid_land, m3$.tid_land)),
  c("vid", "date"),
  "04 vid_date ±1"
)

# Step 5: ±2 days, port dropped
# All newly resolved trips use exactly a 2-day offset (systematic recording lag).
# ±3 drops the signal-to-ambiguity ratio from 11:1 to ~2:1; not worth the risk.
m5 <- try_match(
  trip     |> filter(!.tid      %in% c(m1$.tid,      m2$.tid,      m3$.tid,      m4$.tid)),
  land_pm2 |> filter(!.tid_land %in% c(m1$.tid_land, m2$.tid_land, m3$.tid_land, m4$.tid_land)),
  c("vid", "date"),
  "05 vid_date ±2"
)

## Steps 6–7: catch-weight matching --------------------------------------------
# gear_mapping has multiple rows per gid (old/new code versions); derive the
# pelagic flag directly from the known gid set rather than joining on gear name.
pelagic_gids <- duckdbfs::open_dataset("data/gear/gear_mapping.parquet") |> collect() |>
  filter(gear %in% c("OTM", "PS", "HMS")) |>
  distinct(gid) |>
  pull(gid)

trip_gear <-
  duckdbfs::open_dataset("data/logbooks/fishing_sample.parquet") |> collect() |>
  mutate(.tid = as.character(.tid)) |>
  count(.tid, gid) |>
  group_by(.tid) |>
  slice_max(n, n = 1, with_ties = FALSE) |>
  ungroup() |>
  mutate(pelagic = gid %in% pelagic_gids) |>
  select(.tid, gid, pelagic)

# Combined land table covering date 0 and ±1 for weight steps
land_w <- bind_rows(land, land_pm1) |> rename(catch_land = catch)

consumed_so_far <- c(m1$.tid_land, m2$.tid_land, m3$.tid_land, m4$.tid_land, m5$.tid_land)

# Step 6: demersal weight match — tight tolerance
unmatched_dem <- trip |>
  filter(!.tid %in% c(m1$.tid, m2$.tid, m3$.tid, m4$.tid, m5$.tid)) |>
  left_join(trip_gear |> select(.tid, pelagic), by = ".tid") |>
  filter(!isTRUE(pelagic))

m6 <- try_match_weight(
  unmatched_dem, land_w, consumed_so_far,
  tol = WEIGHT_TOL_DEMERSAL, gap = WEIGHT_MIN_GAP,
  type = "06 weight_dem"
)

# Step 7: pelagic weight match — loose tolerance; expect ~0 matches
unmatched_pel <- trip |>
  filter(!.tid %in% c(m1$.tid, m2$.tid, m3$.tid, m4$.tid, m5$.tid, m6$.tid)) |>
  left_join(trip_gear |> select(.tid, pelagic), by = ".tid") |>
  filter(isTRUE(pelagic))

m7 <- try_match_weight(
  unmatched_pel, land_w,
  c(consumed_so_far, m6$.tid_land),
  tol = WEIGHT_TOL_PELAGIC, gap = WEIGHT_MIN_GAP,
  type = "07 weight_pel"
)

## Step 8: expected-date match (wrong-date entries) ----------------------------
# Fit approxfun curves (tid_num → expected_date) per tid range from matched
# trips. Flag unmatched trips with large deviations; search for a landing on
# the expected date. Yield ~36 matches; primary value is the date_error_days
# QA column on unmatched trips.
date_origin <- as.Date("2007-01-01")

build_approx <- function(ref_trip) {
  approxfun(
    as.numeric(ref_trip$.tid),
    as.numeric(ref_trip$date - date_origin),
    rule = 2
  )
}

matched_trip <- trip |>
  filter(.tid %in% c(m1$.tid, m2$.tid, m3$.tid, m4$.tid, m5$.tid, m6$.tid, m7$.tid))

approx_afli_pos <- build_approx(matched_trip |>
  filter(schema == "afli", as.numeric(.tid) > 0))
approx_afli_neg <- build_approx(matched_trip |>
  filter(schema == "afli", as.numeric(.tid) < 0, as.numeric(.tid) > -1e9))
approx_fs <- build_approx(matched_trip |>
  filter(schema == "fs_afladagbok"))

unmatched_post7 <- trip |>
  filter(!.tid %in% c(m1$.tid, m2$.tid, m3$.tid, m4$.tid, m5$.tid, m6$.tid, m7$.tid)) |>
  filter(year(date) >= 2007) |>
  mutate(tid_num = as.numeric(.tid)) |>
  mutate(expected_date = date_origin + case_when(
    schema == "fs_afladagbok"                      ~ approx_fs(tid_num),
    schema == "afli" & tid_num > 0                 ~ approx_afli_pos(tid_num),
    schema == "afli" & tid_num < 0 & tid_num > -1e9 ~ approx_afli_neg(tid_num),
    TRUE                                           ~ NA_real_
  )) |>
  mutate(date_error_days = as.numeric(date - expected_date))

land_exp <- seq(-DATE_ERR_WINDOW, DATE_ERR_WINDOW) |>
  purrr::map(\(d) land |> mutate(exp_date = date + d)) |>
  list_rbind()

m8 <- unmatched_post7 |>
  filter(abs(date_error_days) > DATE_ERR_THRESHOLD, !is.na(expected_date)) |>
  rename(exp_date = expected_date) |>
  select(.tid, vid, schema, exp_date, catch) |>
  inner_join(
    land_exp |> filter(!.tid_land %in% c(consumed_so_far, m6$.tid_land, m7$.tid_land)),
    by = c("vid", "exp_date")
  ) |>
  group_by(.tid) |>
  filter(n_distinct(.tid_land) == 1) |>
  slice(1) |>
  ungroup() |>
  mutate(match_type = "08 expected_date")

## Assemble lid_map -------------------------------------------------------------
lid_map <- bind_rows(m1, m2, m3, m4, m5, m6, m7, m8) |>
  select(.tid, schema, .tid_land, match_type)

# Summary: matches per step and cumulative coverage
lid_map |>
  count(match_type) |>
  mutate(cumulative = cumsum(n)) |>
  print()

## Write output -----------------------------------------------------------------
trip_out <- duckdbfs::open_dataset("data/logbooks/trip.parquet") |> collect() |>
  left_join(lid_map, by = c(".tid", "schema"))

lid_map  |> duckdbfs::write_dataset("data/logbooks/lid_map.parquet")
trip_out |> duckdbfs::write_dataset("data/logbooks/trip.parquet")
