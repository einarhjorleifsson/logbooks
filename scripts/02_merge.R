# Merge afli (legacy) and fs_afladagbok into unified tables
# Input:  data/afli/{trip,station,fishing_sample,catch}.parquet
#         data/fs_afladagbok/{trip,station,fishing_sample,catch}.parquet
# Output: data/merged/{trip,station,fishing_sample,catch}.parquet
#
# Decision rule (two-tier priority):
#   Tier 1 — if (vid, date) present in afli            → use afli
#   Tier 2 — not in afli, but present in fs_afladagbok → use fs_afladagbok
#
# Rationale:
#   afli > fs_afladagbok: afli mobile-gear timing is derived from the on-bottom
#   clock and is more accurate than the electronic timestamps in fs_afladagbok.
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

# Load -------------------------------------------------------------------------
afli_trip           <- read_parquet("data/afli/trip.parquet")
afli_station        <- read_parquet("data/afli/station.parquet")
afli_fishing_sample <- read_parquet("data/afli/fishing_sample.parquet")
afli_catch          <- read_parquet("data/afli/catch.parquet")

fs_trip           <- read_parquet("data/fs_afladagbok/trip.parquet")
fs_station        <- read_parquet("data/fs_afladagbok/station.parquet")
fs_fishing_sample <- read_parquet("data/fs_afladagbok/fishing_sample.parquet")
fs_catch          <- read_parquet("data/fs_afladagbok/catch.parquet")

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
dir.create("data/merged", showWarnings = FALSE, recursive = TRUE)
merged_trip           |> write_parquet("data/merged/trip.parquet")
merged_station        |> write_parquet("data/merged/station.parquet")
merged_fishing_sample |> write_parquet("data/merged/fishing_sample.parquet")
merged_catch          |> write_parquet("data/merged/catch.parquet")
