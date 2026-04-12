# Merge afli (legacy), fs_afladagbok, and adb (live feed) into unified tables
# Input:  data/afli/*.parquet, data/fs_afladagbok/*.parquet, data/adb/*.parquet
#         data/gear/gear_mapping.parquet
# Output: data/merged/trip.parquet, station.parquet, catch.parquet
#
# Decision rule (priority order):
#   Tier 1 — if (vid, date) present in afli            → use afli
#   Tier 2 — not in afli, but present in fs_afladagbok → use fs_afladagbok
#   Tier 3 — not in afli or fs_afladagbok              → use adb
#
# Rationale for tier ordering:
#   afli > fs: afli mobile-gear timing (t1/t2) is derived from the on-bottom
#     clock and is more accurate than electronic timestamps in fs_afladagbok.
#   fs > adb: fs_afladagbok carries native "new" gid codes and fully documented
#     coordinate conversion; adb gear codes are old-schema and coordinates are
#     considered less reliable.
#
# Date bounds applied:
#   afli          — no lower bound; upper: <= DATE_MAX
#   fs_afladagbok — lower: >= FS_DATE_MIN; upper: <= FS_DATE_MAX (static dump)
#   adb           — lower: >= ADB_DATE_MIN; upper: <= DATE_MAX

library(tidyverse)
library(nanoparquet)

# Parameters -------------------------------------------------------------------
ADB_DATE_MIN <- as.Date("2020-01-01")
FS_DATE_MIN  <- as.Date("2020-01-01")
FS_DATE_MAX  <- as.Date("2025-12-31")
DATE_MAX     <- as.Date("2026-12-31")

# Load -------------------------------------------------------------------------
afli_trip    <- read_parquet("data/afli/trip.parquet")
afli_station <- read_parquet("data/afli/station.parquet")
afli_catch   <- read_parquet("data/afli/catch.parquet")

fs_trip    <- read_parquet("data/fs_afladagbok/trip.parquet")
fs_station <- read_parquet("data/fs_afladagbok/station.parquet")
fs_catch   <- read_parquet("data/fs_afladagbok/catch.parquet")

adb_trip    <- read_parquet("data/adb/trip.parquet")
adb_station <- read_parquet("data/adb/station.parquet")
adb_catch   <- read_parquet("data/adb/catch.parquet")

# Old→new gear code lookup (for adb, which carries old codes in gid_old)
gear_old_to_new <- read_parquet("data/gear/gear_mapping.parquet") |>
  filter(version == "old") |>
  select(gid_old = gid, gid = map)

# Apply date bounds ------------------------------------------------------------
afli_station <- afli_station |> filter(date <= DATE_MAX)
fs_station   <- fs_station   |> filter(date >= FS_DATE_MIN, date <= FS_DATE_MAX)
adb_station  <- adb_station  |> filter(date >= ADB_DATE_MIN, date <= DATE_MAX)

# Subset trips to those that still have stations after date filtering
afli_trip <- afli_trip |> semi_join(afli_station |> distinct(.tid), by = ".tid")
fs_trip   <- fs_trip   |> semi_join(fs_station   |> distinct(.tid), by = ".tid")
adb_trip  <- adb_trip  |> semi_join(adb_station  |> distinct(.tid), by = ".tid")

# Tier 1: afli — full coverage -------------------------------------------------
# vid lives on trip; attach to station for (vid, date) indexing
afli_vid_date <- afli_station |>
  left_join(afli_trip |> select(.tid, vid), by = ".tid") |>
  distinct(vid, date)

# Tier 2: fs_afladagbok — vessel-days not covered by afli ----------------------
fs_only_station <- fs_station |>
  left_join(fs_trip |> select(.tid, vid), by = ".tid") |>
  anti_join(afli_vid_date, by = c("vid", "date")) |>
  select(-vid)

fs_tids_kept  <- fs_only_station |> distinct(.tid)
fs_only_trip  <- fs_trip  |> semi_join(fs_tids_kept, by = ".tid")
fs_only_catch <- fs_catch |> semi_join(fs_only_station |> distinct(.sid), by = ".sid")

# Vessel-days covered by tier 2 (used to exclude from tier 3)
fs_only_vid_date <- fs_only_station |>
  left_join(fs_trip |> select(.tid, vid), by = ".tid") |>
  distinct(vid, date)

# Tier 3: adb — vessel-days not covered by afli OR fs_afladagbok ---------------
adb_only_station <- adb_station |>
  left_join(adb_trip |> select(.tid, vid), by = ".tid") |>
  anti_join(afli_vid_date,    by = c("vid", "date")) |>
  anti_join(fs_only_vid_date, by = c("vid", "date")) |>
  # Derive new-schema gid from old-schema gid_old via gear mapping
  left_join(gear_old_to_new, by = "gid_old") |>
  select(-vid)

adb_tids_kept  <- adb_only_station |> distinct(.tid)
adb_only_trip  <- adb_trip  |> semi_join(adb_tids_kept, by = ".tid")
adb_only_catch <- adb_catch |> semi_join(adb_only_station |> distinct(.sid), by = ".sid")

# Merge ------------------------------------------------------------------------
# Coerce .tid to character for all schemas before binding
merged_trip <- bind_rows(
  afli_trip    |> mutate(.tid = as.character(.tid)),
  fs_only_trip |> mutate(.tid = as.character(.tid)),
  adb_only_trip
)

merged_station <- bind_rows(
  afli_station    |> mutate(.tid = as.character(.tid)),
  fs_only_station |> mutate(.tid = as.character(.tid)),
  adb_only_station
)

merged_catch <- bind_rows(afli_catch, fs_only_catch, adb_only_catch)

# Summary ----------------------------------------------------------------------
cat(sprintf("afli stations (filtered):   %d\n", nrow(afli_station)))
cat(sprintf("fs-only stations:           %d\n", nrow(fs_only_station)))
cat(sprintf("adb-only stations:          %d\n", nrow(adb_only_station)))
cat(sprintf("merged stations:            %d\n", nrow(merged_station)))
cat(sprintf("afli trips (filtered):      %d\n", nrow(afli_trip)))
cat(sprintf("fs-only trips:              %d\n", nrow(fs_only_trip)))
cat(sprintf("adb-only trips:             %d\n", nrow(adb_only_trip)))
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
merged_trip    |> write_parquet("data/merged/trip.parquet")
merged_station |> write_parquet("data/merged/station.parquet")
merged_catch   |> write_parquet("data/merged/catch.parquet")
