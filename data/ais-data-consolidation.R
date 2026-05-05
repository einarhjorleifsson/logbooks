# Consolidate all AIS / GPS sources into a single Icelandic-vessel track dataset.
#
# Inputs:
#   data-dump/ais/stk/trail/            — STK AIS pings (mid-keyed, all vessels)
#   data-raw/ais/astd/                  — ASTD commercial AIS (all flags, parquet)
#   data-raw/ais/astdB/                 — ASTDB commercial AIS (all flags, parquet)
#   data-raw/logbooks/afli/sensor_GPS.parquet — AIS-corrected GPS logger positions
#   data-dump/ais/EmodNet/is2017.csv    — EmodNet AIS snapshot (Iceland, 2017 only)
#   data/vessels/stk_vessel_match.parquet — mid → vid temporal lookup
#   data/ports/ports.gpkg               — port polygons for harbour tagging
#
# Output:
#   data/trail/   — Hive-partitioned parquet (by year); ~566 M rows (Icelandic vessels)
#
# All heavy lifting is done lazily in DuckDB; data never fully enter R memory.
library(tictoc)
tic()
library(sf)
library(duckdb)
library(duckdbfs)
library(tidyverse)
duckdbfs::load_spatial()

ports <-
  duckdbfs::open_dataset("../kaenugardr/ports_all.gpkg", format = "sf") |>
  filter(source == "einar") |>
  select(pid, geom)

# mid → vid match table; used for STK (temporal interval join) and as the
# authoritative MMSI → vid lookup for the commercial providers.
vid <- duckdbfs::open_dataset("data/vessels/stk_vessel_match.parquet")

# stk --------------------------------------------------------------------------
# STK is Iceland's domestic AIS receiver network. Pings are keyed by `mid`
# (a STK-internal mobile ID), so we resolve to vid via a temporal interval join:
# each mid may map to different vessels over time as transponders are reused.
stk <- duckdbfs::open_dataset("data-dump/ais/stk/trail") |>
  inner_join(vid |> select(vid, mmsi, mid, d1, d2) |> filter(!is.na(vid)),
             by = join_by(mid, between(time, d1, d2))) |>
  select(vid, mmsi, time, lon, lat, speed, heading)

# astd -------------------------------------------------------------------------
# ASTD is a global commercial AIS feed. The raw data cover all flags, so we
# filter to 9-digit MMSIs starting with "251" (Iceland's MID) before joining
# to vid. Speed is not directly available; we derive it from distance/time
# between consecutive pings and convert m/s → knots.
astd  <- open_dataset("data-raw/ais/astd") |>
  mutate(mmsi = as.character(mmsi),
         nc = nchar(mmsi)) |>
  filter(nc == 9 & str_sub(mmsi, 1, 3) == "251") |>
  inner_join(vid |> select(mmsi, vid) |> distinct() |> filter(!is.na(mmsi))) |>
  mutate(speed = dist_nextpoint / sec_nextpoint * 1.94384449) |>
  select(vid, mmsi, time = date_time_utc,
         lon = longitude, lat = latitude,
         speed,
         imo = imonumber) |>
  # IMO numbers ≤ 0 are placeholder/null values in this feed
  mutate(imo = ifelse(imo <= 0, NA, imo))

# astdB ------------------------------------------------------------------------
# ASTDB is a second commercial AIS feed. Filtered to Iceland (cc_iso3 == "ISL")
# before joining; this provider includes COG in addition to true heading.
astdB <- open_dataset("data-raw/ais/astdB") |>
  filter(cc_iso3 == "ISL") |>
  mutate(mmsi = as.character(mmsi)) |>
  inner_join(vid |> filter(!is.na(mmsi)) |> select(mmsi, vid) |> distinct()) |>
  select(vid, mmsi, time = date_time_utc,
         lon, lat,
         speed,
         heading = true_heading, cog)

# wacky ------------------------------------------------------------------------
# GPS logger positions from the afli `rafr_sjalfvirkir_maelar` table, after
# AIS-based coordinate recovery (01_afli_siritar.R). These replace the wacky
# DDMMmm→DMS-misconverted originals and extend coverage for vessels with poor
# AIS reception from the commercial feeds.
wacky <-
  open_dataset("data-raw/logbooks/afli/sensor_GPS.parquet") |>
  inner_join(vid |> filter(!is.na(mmsi), !is.na(vid)) |> select(mmsi, vid) |> distinct()) |>
  select(vid, mmsi, time, lon, lat, speed, heading)

# emodnet ----------------------------------------------------------------------
# EmodNet snapshot: Iceland-only extract for 2017. Speed is recomputed from
# consecutive pings because the feed does not carry it directly.
# Note: an additional 2019 file exists (AdditionalMMSI20190327.csv) but was
# excluded due to parsing issues.
emn <-
  read_csv("data-dump/ais/EmodNet/is2017.csv") |>
  select(mmsi, time = utime, lon, lat, heading = trueheading, cog) |>
  mutate(mmsi = as.character(mmsi)) |>
  left_join(vid |> filter(!is.na(mmsi), !is.na(vid)) |> select(mmsi, vid) |> distinct(mmsi, .keep_all = TRUE) |>
              collect()) |>
  arrange(vid, time) |>
  group_by(vid) |>
  mutate(speed = ramb::rb_speed(lat, lon, time)) |>
  ungroup() |>
  select(vid, mmsi, time, lon, lat, speed, everything()) |>
  duckdbfs::as_dataset() |>
  # POSIXct registered via as_dataset() lands as TIMESTAMPTZ in DuckDB; cast to
  # plain TIMESTAMP so the union with other sources stays type-consistent.
  mutate(time = dplyr::sql("time::TIMESTAMP"))

# bind -------------------------------------------------------------------------
# Union all five sources; `provider` tag lets downstream users trace each ping
# back to its origin, which matters for coverage gap analysis.
d <-
  union(stk   |> mutate(provider = "stk"),
        astd  |> mutate(provider = "astd")) |>
  union(astdB |> mutate(provider = "astdB")) |>
  union(wacky |> mutate(provider = "wacky")) |>
  union(emn   |> mutate(provider = "emodnet"))

# ## add harbour -----------------------------------------------------------------
# # Tag each ping with a port ID where the position falls inside a port polygon.
# # Left join preserves pings at sea (pid = NA).
# print("adding harbour")
# d <-
#   d |>
#   mutate(geom = ST_POINT(lon, lat)) |>
#   duckdbfs::spatial_join(ports, by = "st_intersects", join = "left") |>
#   select(-geom_1)

# write out --------------------------------------------------------------------
# Partition by year so downstream queries can apply partition pruning.
# The loop materialises one year at a time to keep peak memory manageable.
years <- 2007:2026
for(y in 1:length(years)) {
  YR <- years[y]
  print(YR)
  d |>
    mutate(year = year(time)) |>
    filter(year == YR) |>
    arrange(vid, time) |>
    # duckdbfs::write_dataset keeps TIMESTAMP as-is; arrow::write_dataset
    # materialises through R POSIXct and re-writes as TIMESTAMPTZ.
    duckdbfs::write_dataset("data/trail", partitioning = "year", format = "parquet")
}

toc()


#
# # issue / sanity test ----------------------------------------------------------
# #  one should have marked all the points in harbour in the above script
# #  here is a sanity test
# # this take too much time
# # d |>
# #   filter(is.na(pid)) |>
# #   select(-pid) |>
# #   duckdbfs::spatial_join(ports, by = "st_intersects", join = "left") |>
# #   select(-geom_1) |>
# #   # I did not expect this
# #   filter(!is.na(pid))
#
# # read from just created data
# ais2 <-
#   open_dataset("data/trail") |>
#   # filter out all points in ports
#   filter(is.na(pid)) |>
#   select(-pid)
# ais2 |>
#   duckdbfs::spatial_join(ports, by = "st_intersects", join = "left") |>
#   select(-geom_1) |>
#   # I did not expect this
#   filter(!is.na(pid))
