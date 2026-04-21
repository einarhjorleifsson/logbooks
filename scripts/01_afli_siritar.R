# Input:
#  data-raw/data-dump/afli/sjalfvirkir_maelar.parquet
#  data-raw/data-dump/afli/rafr_sjalfvirkir_maelar.parquet
#  data/merged/trip.parquet           — to get vid and filter to afli schema
#  data/merged/fishing_sample.parquet — to map .sid → .tid
#  data/trail                         — AIS positions, copy of /u3/geo/fishydata
# Output:
#  data/afli/sensor_GPS.parquet

# Summary:
#  Wacky coordinates in the afli GPS logger tables (sjalfvirkir_maelar,
#  rafr_sjalfvirkir_maelar) are corrected by linear interpolation from AIS
#  tracks, which are free of the DDMMmm encoding error. The original wacky
#  lon/lat are retained as w_lon/w_lat for audit purposes.
#  approx(..., rule = 2): GPS timestamps outside the AIS window are extrapolated
#  from the nearest AIS endpoint; NA lon/lat only for vessels with no AIS in
#  that year (dropped by the vid filter before interpolation).
#  dt_sec: seconds between the two AIS fixes that bracket each GPS timestamp
#  (i.e. the actual interpolation interval). NA for records outside the AIS
#  window (extrapolated). Larger dt_sec → less reliable interpolated position.

library(arrow)
library(duckdbfs)
library(tidyverse)

# the two datasources are nearly identical; gps1 covers a longer timespan
gps1 <- read_parquet("data-raw/data-dump/afli/sjalfvirkir_maelar.parquet")
gps2 <- read_parquet("data-raw/data-dump/afli/rafr_sjalfvirkir_maelar.parquet")

# build station-level lookup: .sid → .tid + vid, afli schema only
# drop schema from fishing_sample select to avoid column duplication
st <-
  read_parquet("data/merged/trip.parquet") |>
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

  res[[i]] <-
    bind_rows(gp, tr) |>
    arrange(vid, time) |>
    # flag trail rows before lon/lat are overwritten by interpolation
    mutate(is_trail = !is.na(lon)) |>
    group_by(vid) |>
    mutate(
      t_num = as.numeric(time),
      # vessels with no AIS coverage in this year: lon/lat remain NA
      lon = if (any(is_trail))
        approx(t_num, lon, t_num, method = "linear", rule = 2, ties = mean)$y
      else
        NA_real_,
      lat = if (any(is_trail))
        approx(t_num, lat, t_num, method = "linear", rule = 2, ties = mean)$y
      else
        NA_real_,
      # dt_sec: span (s) between the two AIS fixes bracketing each GPS record;
      # rule = 1 → NA for records outside the AIS window (extrapolated, not bracketed)
      t_before = if (any(is_trail))
        approx(t_num[is_trail], t_num[is_trail], t_num,
               method = "constant", f = 0, rule = 1, ties = mean)$y
      else
        NA_real_,
      t_after  = if (any(is_trail))
        approx(t_num[is_trail], t_num[is_trail], t_num,
               method = "constant", f = 1, rule = 1, ties = mean)$y
      else
        NA_real_,
      dt_sec = t_after - t_before
    ) |>
    ungroup() |>
    select(-is_trail, -t_num, -t_before, -t_after) |>
    filter(source %in% c("gps1", "gps2")) |>
    select(.tid, .sid, vid, time, lon, lat, speed, heading, everything())
  i <- i + 1

}

res <- bind_rows(res)
res |> write_parquet("data/afli/sensor_GPS.parquet")

# QC ---------------------------------------------------------------------------
# Vessel where no approximation done
res |>
  group_by(vid) |>
  summarise(records = n(),
            n_approx = sum(!is.na(lon)),
            p_missing = 1 -  n_approx / records) |>
  arrange(-p_missing) |>
  slice(1:20)
# comment: check the vid being NA, should not happen

