# Merge afli (legacy) and fs_afladagbok into unified tables and generate a
#  match to landings data
# Input:  data-raw/logbooks/afli/{trip,station,fishing_sample,catch}.parquet
#         data-raw/logbooks/fs_afladagbok/{trip,station,fishing_sample,catch}.parquet
# Output: data/logbooks/{trip,station,fishing_sample,catch}.parquet
#
# Decision rule (two-tier priority):
#   Tier 1 — if (vid, date) present in afli            → use afli
#   Tier 2 — not in afli, but present in fs_afladagbok → use fs_afladagbok
#
# Rationale:
#   afli > fs_afladagbok: afli mobile-gear timing is derived from the on-bottom
#   clock - when fs_afladagbok was instigated timestamps and/or duration were
#.  missing in fs_afladagbok.
#
# Date bounds applied:
#   afli          — no lower bound; upper: <= DATE_MAX
#   fs_afladagbok — lower: >= FS_DATE_MIN; upper: <= FS_DATE_MAX (static dump)

library(tidyverse)
library(nanoparquet)

# Parameters -------------------------------------------------------------------
FS_DATE_MIN <- as.Date("2020-01-01")
FS_DATE_MAX <- as.Date("2025-12-31")
DATE_MAX    <- as.Date("2026-12-31")

# Catch-weight match tolerances (steps 6 & 7) ----------------------------------
# Matches are scored as abs(log(catch_logbook / catch_landing)).
# A score of 0 is a perfect weight match; log(1.2) ≈ 0.18 means ≤ 20% apart.
# WEIGHT_MIN_GAP: minimum score difference between the best and second-best
# candidate — guards against accepting near-ties as unambiguous.
WEIGHT_TOL_DEMERSAL <- log(1.3)   # accept if within ~30 % of landing weight
WEIGHT_TOL_PELAGIC  <- log(2.0)   # accept if within ~100 % (2×); catch transfers
WEIGHT_MIN_GAP      <- log(1.5)   # best must be ≥ 1.5× closer than runner-up
DATE_ERR_THRESHOLD  <- 35L        # days; larger deviation flags a likely wrong-date entry
DATE_ERR_WINDOW     <- 3L         # ± days around expected date to search for a landing

# Load -------------------------------------------------------------------------
afli_trip           <- read_parquet("data-raw/logbooks/afli/trip.parquet")
afli_station        <- read_parquet("data-raw/logbooks/afli/station.parquet")
afli_fishing_sample <- read_parquet("data-raw/logbooks/afli/fishing_sample.parquet")
afli_catch          <- read_parquet("data-raw/logbooks/afli/catch.parquet")

fs_trip           <- read_parquet("data-raw/logbooks/fs_afladagbok/trip.parquet")
fs_station        <- read_parquet("data-raw/logbooks/fs_afladagbok/station.parquet")
fs_fishing_sample <- read_parquet("data-raw/logbooks/fs_afladagbok/fishing_sample.parquet")
fs_catch          <- read_parquet("data-raw/logbooks/fs_afladagbok/catch.parquet")

# Apply date bounds ------------------------------------------------------------
afli_station <- afli_station |> filter(date <= DATE_MAX)
fs_station   <- fs_station   |> filter(date >= FS_DATE_MIN, date <= FS_DATE_MAX)

# Subset all tables to stations/trips surviving the date filter
afli_trip           <- afli_trip           |> semi_join(afli_station |> distinct(.tid), by = ".tid")
afli_fishing_sample <- afli_fishing_sample |> semi_join(afli_station |> distinct(.sid), by = ".sid")
afli_catch          <- afli_catch          |> semi_join(afli_station |> distinct(.sid), by = ".sid")

fs_trip           <- fs_trip           |> semi_join(fs_station |> distinct(.tid), by = ".tid")
fs_fishing_sample <- fs_fishing_sample |> semi_join(fs_station |> distinct(.sid), by = ".sid")
fs_catch          <- fs_catch          |> semi_join(fs_station |> distinct(.sid), by = ".sid")

# Tier 1: afli — full coverage -------------------------------------------------
afli_vid_date <- afli_station |>
  left_join(afli_trip |> select(.tid, vid), by = ".tid") |>
  distinct(vid, date)

# Tier 2: fs_afladagbok — vessel-days not covered by afli ----------------------
fs_only_station <- fs_station |>
  left_join(fs_trip |> select(.tid, vid), by = ".tid") |>
  anti_join(afli_vid_date, by = c("vid", "date")) |>
  select(-vid)

fs_sids_kept <- fs_only_station |> distinct(.sid)
fs_tids_kept <- fs_only_station |> distinct(.tid)

fs_only_trip           <- fs_trip           |> semi_join(fs_tids_kept, by = ".tid")
fs_only_fishing_sample <- fs_fishing_sample |> semi_join(fs_sids_kept, by = ".sid")
fs_only_catch          <- fs_catch          |> semi_join(fs_sids_kept, by = ".sid")

# Merge ------------------------------------------------------------------------
merged_trip <- bind_rows(
  afli_trip    |> mutate(.tid = as.character(.tid)),
  fs_only_trip |> mutate(.tid = as.character(.tid))
)

merged_station <- bind_rows(
  afli_station    |> mutate(.tid = as.character(.tid)),
  fs_only_station |> mutate(.tid = as.character(.tid))
)

merged_fishing_sample <- bind_rows(
  afli_fishing_sample    |> mutate(.tid = as.character(.tid)),
  fs_only_fishing_sample |> mutate(.tid = as.character(.tid))
)

merged_catch <- bind_rows(afli_catch, fs_only_catch)

# Summary ----------------------------------------------------------------------
cat(sprintf("afli stations (filtered):   %d\n", nrow(afli_station)))
cat(sprintf("fs-only stations:           %d\n", nrow(fs_only_station)))
cat(sprintf("merged stations:            %d\n", nrow(merged_station)))
cat(sprintf("afli trips (filtered):      %d\n", nrow(afli_trip)))
cat(sprintf("fs-only trips:              %d\n", nrow(fs_only_trip)))
cat(sprintf("merged trips:               %d\n", nrow(merged_trip)))
cat(sprintf("merged catch:               %d\n", nrow(merged_catch)))

cat("\nYear-by-year station source (2018+):\n")
merged_station |>
  mutate(yr = lubridate::year(date)) |>
  count(schema, yr) |>
  pivot_wider(names_from = schema, values_from = n, values_fill = 0L) |>
  arrange(yr) |>
  filter(yr >= 2018) |>
  print()

# Export -----------------------------------------------------------------------
dir.create("data/logbooks", showWarnings = FALSE, recursive = TRUE)
merged_trip           |> write_parquet("data/logbooks/trip.parquet")
merged_station        |> write_parquet("data/logbooks/station.parquet")
merged_fishing_sample |> write_parquet("data/logbooks/fishing_sample.parquet")
merged_catch          |> write_parquet("data/logbooks/catch.parquet")


# logbooks-landings_match ------------------------------------------------------
#
# Build a `.tid` → `.tid_land` crosswalk between the logbook trip table and
# the landing register (aflagrunnur).
#
# Inputs:
#   data/landings/landings.parquet
#.  data/landings/catch.parquet
#   data/logbooks/trip.parquet
#   data/logbooks/station.parquet
#   data/logbooks/catch.parquet
#
# Output:
#   data/logbooks/lid_map.parquet  — columns: .tid, .lid, match_type

library(arrow)
library(tidyverse)

## Landing register -------------------------------------------------------------
# One row per (vid × date × hid × port); catch summed.
# Multiple .lids on the same key are collapsed to min(.lid).
land <-
  read_parquet("data/landings/landings.parquet") |>
  filter(vid > 0, !between(vid, 3700, 4999)) |>
  filter(date <= ymd("2025-12-12")) |>
  left_join(read_parquet("data/landings/catch.parquet") |>
              group_by(.tid) |>
              summarise(catch = sum(catch))) |>
  rename(.tid_land = .tid)

## Logbook trips with total catch -----------------------------------------------
trip <-
  read_parquet("data/logbooks/trip.parquet") |>
  mutate(date = as_date(T2)) |>
  # filter(date >= ymd("2007-09-01"), date <= ymd("2025-12-12")) |>
  # filter(vid > 0, !between(vid, 3700, 4999)) |>
  left_join(
    read_parquet("data/logbooks/catch.parquet") |>
      left_join(
        read_parquet("data/logbooks/station.parquet") |> select(.sid, .tid, schema)
      ) |>
      filter(!is.na(.tid)) |>
      group_by(.tid) |>
      summarise(catch = sum(as.numeric(catch), na.rm = TRUE), .groups = "drop"),
    by = ".tid"
  ) |>
  filter(catch > 0) |>
  select(.tid, vid, date, hid = hid2, port, catch, schema)

## Cascade match ----------------------------------------------------------------
# Six-step cascade in decreasing key specificity.
# Each step operates only on trips and landings not yet consumed by earlier steps.
# Steps 1–4 require an unambiguous key match (single .tid_land candidate).
# Steps 5–6 revisit remaining ambiguous trips using catch weight as a tiebreaker.
#
#   Step 1 — exact (vid, date, hid): tightest key; highest confidence
#   Step 2 — (vid, date ± 1, hid): date shifted ±1 day, port still required;
#             catches trips where logbook and landing date differ by one day
#             (e.g., overnight trips straddling midnight)
#   Step 3 — (vid, date, port dropped): same day, port relaxed;
#             unambiguous if only one landing exists for that vessel-day
#   Step 4   — (vid, date ± 1, port dropped): both date and port relaxed
#   Step 5 — (vid, date ± 2, port dropped): extends to ±2 days; empirically
#               adds ~3,400 unambiguous matches at 11:1 signal-to-ambiguity ratio;
#               all resolved via exactly 2-day offset (systematic recording lag);
#               ±3 drops ratio to ~2:1 and is not worth the added ambiguity
#   Step 6 — demersal catch-weight match: among remaining ambiguous demersal trips,
#             accept the landing whose weight is within WEIGHT_TOL_DEMERSAL and
#             clearly closer than any other candidate (gap ≥ WEIGHT_MIN_GAP)
#   Step 7 — pelagic catch-weight match: same logic with WEIGHT_TOL_PELAGIC;
#             expect few matches because pelagic trips often transfer catch at sea
#   Step 8 — expected-date match: for trips whose recorded date deviates by more
#             than DATE_ERR_THRESHOLD days from the date implied by .tid ordering,
#             try matching on the tid-interpolated expected date ± DATE_ERR_WINDOW;
#             accept only unambiguous single-landing hits
try_match <- function(unmatched, land_tbl, by, type) {
  unmatched |>
    inner_join(land_tbl, by = by) |>
    group_by(.tid) |>
    filter(n_distinct(.tid_land) == 1) |>
    slice(1) |>
    ungroup() |>
    mutate(match_type = type)
}

# Step 1: exact key
m1 <- try_match(trip, land, c("vid", "date", "hid"), "01 exact")

# Step 2: date ± 1 day, exact port
land_pm1 <- bind_rows(
  land |> mutate(date = date - 1),
  land |> mutate(date = date + 1)
)
m2 <- try_match(
  trip  |> filter(!.tid %in% m1$.tid),
  land_pm1 |> filter(!.tid_land %in% m1$.tid_land),
  c("vid", "date", "hid"),
  "02 exact ±1 day"
)

# Step 3: vid + date only (port dropped), no date relaxation
m3 <- try_match(
  trip |> filter(!.tid %in% c(m1$.tid, m2$.tid)),
  land |> filter(!.tid_land %in% c(m1$.tid_land, m2$.tid_land)),
  c("vid", "date"),
  "03 vid_date"
)

# Step 4: date ± 1 day, port dropped — catches trips where both the recorded
# landing date and port diverge from the logbook (e.g., port recorded at
# departure in the logbook but at arrival in the landing register)
m4 <- try_match(
  trip |> filter(!.tid %in% c(m1$.tid, m2$.tid, m3$.tid)),
  land_pm1 |> filter(!.tid_land %in% c(m1$.tid_land, m2$.tid_land, m3$.tid_land)),
  c("vid", "date"),
  "04 vid_date ±1"
)

# Step 5: date ± 2 days, port dropped
# Empirically, ±2 adds ~3,400 unambiguous matches for only ~300 new ambiguous cases
# (11:1 ratio). All newly resolved trips use exactly a 2-day offset, suggesting a
# systematic recording lag rather than anything trip-length can predict. ±3 drops
# the ratio to ~2:1 and is not worth the added ambiguity risk.
land_pm2 <- bind_rows(
  land |> mutate(date = date - 2),
  land |> mutate(date = date + 2)
)
m5 <- try_match(
  trip |> filter(!.tid %in% c(m1$.tid, m2$.tid, m3$.tid, m4$.tid)),
  land_pm2 |> filter(!.tid_land %in% c(m1$.tid_land, m2$.tid_land, m3$.tid_land, m4$.tid_land)),
  c("vid", "date"),
  "05 vid_date ±2"
)

## Catch-weight matching (steps 6 & 7) -----------------------------------------
# Steps 1–4 reject trips where a key matches multiple landing records (ambiguous).
# Steps 5 & 6 revisit those ambiguous trips and use the logbook total catch weight
# to pick the most likely landing. Only accepted when:
#   (a) the best candidate's weight ratio is within tolerance, AND
#   (b) the score gap to the second-best candidate exceeds WEIGHT_MIN_GAP
#       (i.e., one candidate is clearly better than the rest).
# Steps are separated by gear type because pelagic trips routinely transfer catch
# at sea, so the logbook weight may differ substantially from any single landing.

# Dominant gear per trip: count stations per (trip, gid), pick the modal gid.
# gear_mapping has multiple rows per gid (old/new code versions), so we derive
# the pelagic flag directly from the known pelagic gid set rather than joining.
pelagic_gids <- read_parquet("data/gear/gear_mapping.parquet") |>
  filter(gear %in% c("OTM", "PS", "HMS")) |>
  distinct(gid) |>
  pull(gid)

trip_gear <-
  read_parquet("data/logbooks/fishing_sample.parquet") |>
  mutate(.tid = as.character(.tid)) |>
  count(.tid, gid) |>
  group_by(.tid) |>
  slice_max(n, n = 1, with_ties = FALSE) |>
  ungroup() |>
  mutate(pelagic = gid %in% pelagic_gids) |>
  select(.tid, gid, pelagic)

# Combined land table covering date 0 and ±1; rename catch to avoid collision
# with the trip catch column during the join
land_w <- bind_rows(land, land_pm1) |> rename(catch_land = catch)

try_match_weight <- function(unmatched, land_tbl, consumed_land, tol, gap, type) {
  unmatched |>
    inner_join(
      land_tbl |> filter(!.tid_land %in% consumed_land),
      by = c("vid", "date")
    ) |>
    # score = abs log-ratio; 0 = perfect weight match
    mutate(score = abs(log(catch / catch_land))) |>
    filter(is.finite(score), score <= tol) |>
    group_by(.tid) |>
    arrange(score, .by_group = TRUE) |>
    # require a clear gap between best and second-best candidate
    filter(n() == 1 | (score[2] - score[1]) >= gap) |>
    slice(1) |>
    ungroup() |>
    mutate(match_type = type)
}

consumed_so_far <- c(m1$.tid_land, m2$.tid_land, m3$.tid_land, m4$.tid_land, m5$.tid_land)

# Step 6: demersal weight match — tight tolerance
# Demersal trips land their full catch; a close weight match is a reliable signal.
unmatched_dem <- trip |>
  filter(!.tid %in% c(m1$.tid, m2$.tid, m3$.tid, m4$.tid, m5$.tid)) |>
  left_join(trip_gear |> select(.tid, pelagic), by = ".tid") |>
  filter(!isTRUE(pelagic))

m6 <- try_match_weight(
  unmatched_dem, land_w, consumed_so_far,
  tol = WEIGHT_TOL_DEMERSAL, gap = WEIGHT_MIN_GAP,
  type = "06 weight_dem"
)

# Step 7: pelagic weight match — loose tolerance
# Pelagic trips routinely transfer catch at sea, so the logbook total rarely
# equals any single landing record. WEIGHT_MIN_GAP still guards against
# near-ties. Expect few or zero matches; the step is retained so the tolerance
# can be tuned without restructuring the cascade.
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

## Step 8: expected-date match (wrong date entries) ----------------------------
# .tid values in afli (both positive and negative Oracle sequences) and
# fs_afladagbok follow rough chronological order. Trips where the recorded date
# deviates substantially from the date implied by neighboring .tid values are
# likely data-entry errors (month or year transposition).
#
# Strategy:
#   1. Fit an approxfun mapping tid_num → expected date using matched trips as
#      the reference curve — one function per tid range (positive afli, negative
#      afli, fs_afladagbok).
#   2. Flag unmatched trips where |recorded_date − expected_date| > DATE_ERR_THRESHOLD.
#   3. For those trips, try matching against landings on expected_date ± DATE_ERR_WINDOW
#      days (wider than steps 1–5 to account for interpolation error).
#   4. Accept only unambiguous matches (single landing candidate).
#
# Yield is small (~tens of trips) but confidence is high: two independent
# signals agree (large tid/date mismatch + unique landing on expected date).
date_origin <- as.Date("2007-01-01")

build_approx <- function(ref_trip) {
  approxfun(
    as.numeric(ref_trip$.tid),
    as.numeric(ref_trip$date - date_origin),
    rule = 2
  )
}

# Fit one reference curve per tid range from all matched trips
matched_trip <- trip |> filter(.tid %in% c(m1$.tid, m2$.tid, m3$.tid, m4$.tid,
                                            m5$.tid, m6$.tid, m7$.tid))

approx_afli_pos <- build_approx(matched_trip |>
  filter(schema == "afli", as.numeric(.tid) > 0))
approx_afli_neg <- build_approx(matched_trip |>
  filter(schema == "afli", as.numeric(.tid) < 0, as.numeric(.tid) > -1e9))
approx_fs <- build_approx(matched_trip |>
  filter(schema == "fs_afladagbok"))

unmatched_post7 <- trip |>
  filter(!.tid %in% c(m1$.tid, m2$.tid, m3$.tid, m4$.tid, m5$.tid, m6$.tid, m7$.tid)) |>
  filter(year(date) >= 2008) |>
  mutate(tid_num = as.numeric(.tid)) |>
  mutate(expected_date = date_origin + case_when(
    schema == "fs_afladagbok"         ~ approx_fs(tid_num),
    schema == "afli" & tid_num > 0    ~ approx_afli_pos(tid_num),
    schema == "afli" & tid_num < 0 &
      tid_num > -1e9                  ~ approx_afli_neg(tid_num),
    TRUE                              ~ NA_real_
  )) |>
  mutate(date_error_days = as.numeric(date - expected_date))

# Build a shifted land table keyed on expected_date rather than recorded date
land_exp <- map_dfr(
  seq(-DATE_ERR_WINDOW, DATE_ERR_WINDOW),
  \(d) land |> mutate(exp_date = date + d)
)

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

lid_map <- bind_rows(m1, m2, m3, m4, m5, m6, m7, m8) |>
  select(.tid, schema, .tid_land, match_type)

# Summary: matches per step and cumulative coverage
lid_map |>
  count(match_type) |>
  mutate(cumulative = cumsum(n)) |>
  print()

trip <-
  trip |>
  left_join(lid_map, by = c(".tid", "schema"))

# match overview
trip |>
  filter(date >= ymd("2007-09-01")) |>
  mutate(match_type = replace_na(match_type, "none")) |>
  count(match_type) |>
  mutate(p = n / sum(n),
         cn = cumsum(n),
         cp = cn / sum(n))
trip |>
  filter(date >= ymd("2007-09-01")) |>
  left_join(merged_fishing_sample |> select(.tid, .sid, schema, gear)) |>
  mutate(match_type = replace_na(match_type, "none")) |>
  count(gear, match_type) |>
  group_by(gear) |>
  mutate(p = round(n / sum(n) * 100, 1),
         p = cumsum(p)) |>
  ungroup() |>
  drop_na() |>
  select(gear, match_type, p) |>
  spread(gear, p)
trip |>
  mutate(year = year(date)) |>
  mutate(match_type = replace_na(match_type, "none")) |>
  count(year, match_type) |>
  group_by(year) |>
  mutate(p = n / sum(n)) |>
  ungroup() |>
  filter(match_type == "none", year >= 2008) |>
  ggplot(aes(year, p)) + geom_point() +
  labs(y = "Proportion no-match")


## Write output -----------------------------------------------------------------
lid_map |> write_parquet("data/logbooks/lid_map.parquet")
trip    |> write_parquet("data/logbooks/trip.parquet")
