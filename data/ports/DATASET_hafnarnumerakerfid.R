library(sf)
library(arrow)
library(tidyverse)
harbours <-
  read_parquet("data-dump/landings/agf/aflagrunnur_v.parquet") |>
  select(starts_with("hafnar")) |>
  distinct() |>
  rename(hid = hafnarnumer) |>
  arrange(hid)
ports <-
  read_sf("data/ports/ports.gpkg") |> st_drop_geometry() |>
  filter(!str_sub(pid, 1, 2) == "FO") |>
  select(hid, port, pid) |>
  full_join(harbours) |>
  mutate(port =
           case_when(is.na(port) ~ hafnarnumer_heiti,
                     .default = port)) |>
  arrange(hid) |>
  select(-hafnarnumer_heiti)
ports |> write_parquet("data/ports/hafnarnumerakerfid.parquet")
