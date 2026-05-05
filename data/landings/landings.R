library(arrow)
library(tidyverse)
ports <- read_parquet("data/ports/hafnarnumerakerfid.parquet")
aflagrunnur <-
  read_parquet("data-dump/landings/agf/aflagrunnur.parquet") |>
  select(.lid = londun_id, vid = skip_numer, date = londun_hefst,
         hid = hafnarnumer, gid = veidarfaeri, .sid = fisktegund,
         .stid = veidistofn,
         catch = magn_oslaegt) |>
  mutate(date = as_date(date)) |>
  group_by(.lid, vid, date, hid, gid, .sid, .stid) |>
  summarise(catch = sum(catch, na.rm = TRUE),
            .groups = "drop") |>
  group_by(.lid, date, vid, hid) |>
  mutate(.tid = min(.lid),
         .before = ".lid") |>
  ungroup()
landings <-
  aflagrunnur |>
  distinct(.tid, vid, date, hid) |>
  left_join(ports |> select(hid, höfn = port))
catch <-
  aflagrunnur |>
  select(.tid, .lid, gid, .sid, .stid, catch)
landings |> duckdbfs::write_dataset("data/landings/landings.parquet")
catch    |> duckdbfs::write_dataset("data/landings/catch.parquet")
