# Input:
#  data-raw/data-dump/afli/sjalfvirkir_maelar.parquet
#  data-raw/data-dump/afli/rafr_sjalfvirkir_maelar.parquet
#  data/merged/trip.parquet          — to get vid and filter to afli schema
#  data/merged/fishing_sample.parquet — to map .sid → .tid
#  data/trail                         — AIS positions, copy of /u3/geo/fishydata
# Output:
#  data/afli/sensor_GPS.parquet

# Summary:
#  Wacky coordinates in the afli GPS logger tables (sjalfvirkir_maelar,
#  rafr_sjalfvirkir_maelar) are corrected by linear interpolation from AIS
#  tracks, which are free of the DDMMmm encoding error. The original wacky
#  lon/lat are retained as w_lon/w_lat for audit purposes.
#  approx(..., rule = 1) is used — positions outside AIS coverage will be NA.
#  dt_sec is the time gap between surrounding rows in the combined gps+AIS
#  sequence; it approximates the interpolation interval (not the GPS record
#  interval). Larger dt_sec → less reliable interpolated position.

library(arrow)
library(duckdbfs)
library(tidyverse)

# the two datasources are nearly identical; gps1 covers a longer timespan
gps1 <- read_parquet("data-raw/data-dump/afli/sjalfvirkir_maelar.parquet")
gps2 <- read_parquet("data-raw/data-dump/afli/rafr_sjalfvirkir_maelar.parquet")

# build station-level lookup: .sid → .tid + vid, afli schema only
# drop schema from fishing_sample select to avoid column duplication
st <- read_parquet("data/merged/trip.parquet") |>
  select(.tid, vid, schema) |>
  distinct() |>
  left_join(read_parquet("data/merged/fishing_sample.parquet") |>
              select(.tid, .sid)) |>
  filter(schema == "afli") |>
  select(-schema)

gps <-
  bind_rows(gps1 |> mutate(source = "gps1"),
            gps2 |> mutate(source = "gps2")) |>
  rename(.sid = visir,
         time = timi,
         w_lat = breidd, w_lon = lengd,
         z = botndypi,
         z_headline = hofudlinudypi,
         t_bottom = botnhiti,
         t_headline = hofudlinuhiti,
         speed = skip_hradi,
         heading = skip_stefna,
         w_speed = vindhradi,
         w_direction = vindstefna) |>
  arrange(.sid, time) |>
  distinct(.sid, time, w_lat, w_lon, z_headline, t_bottom, t_headline,
           speed, heading, w_speed, w_direction, .keep_all = TRUE) |>
  left_join(st)

trail <-
  open_dataset("data/trail") |>
  filter(.cid > 0) |>
  select(vid, time, lon, lat)

res <- list()
i <- 1
for (Y in 2008:2023) {

  gp <- gps |> filter(year(time) == Y)
  vids <- gp$vid |> unique()
  tr <- trail |> filter(year(time) == Y) |> collect() |> filter(vid %in% vids)
  vids <- tr$vid |> unique()
  gp <- gp |> filter(vid %in% vids)
  res[[i]] <-
    bind_rows(gp, tr) |>
    arrange(vid, time) |>
    mutate(y = 1:n()) |>
    group_by(vid) |>
    mutate(lon = approx(y, lon, y, method = "linear", rule = 1, f = 0, ties = mean)$y,
           lat = approx(y, lat, y, method = "linear", rule = 1, f = 0, ties = mean)$y,
           # time gap between adjacent rows in the combined gps+AIS sequence;
           # approximates the interpolation interval for each GPS record
           dt_sec = as.numeric(difftime(time, lag(time), units = "secs"))) |>
    ungroup() |>
    select(-y) |>
    filter(source %in% c("gps1", "gps2")) |>
    select(.tid, .sid, vid, time, lon, lat, speed, heading, everything())
  i <- i + 1

}

res <- bind_rows(res)
res |> write_parquet("data/afli/sensor_GPS.parquet")
