library(nanoparquet)
library(tidyverse)
library(omar)
con <- connect_mar()

# orri -------------------------------------------------------------------------
tbl_mar(con, "orri.veidarfaeri") |>
  arrange(veidarfaeri) |>
  select(-c(snt, snn, sbt, sbn)) |>
  collect() |>
  write_parquet("data-raw/data-dump/gear/orri_veidarfaeri.parquet")
tbl_mar(con, "orri.veidarfaeri_ny") |>
  collect() |>
  write_parquet("data-raw/data-dump/gear/orri_veidarfaeri_ny.parquet")

# gear -------------------------------------------------------------------------
tbl_mar(con, "gear.veidarfaeri_v") |>
  collect() |>
  arrange(veidarfaeri_nr) |>
  write_parquet("data-raw/data-dump/gear/gear_veidarfaeri_v.parquet")
tbl_mar(con, "gear.fishing_gear") |>
  collect() |>
  arrange(fishing_gear_no) |>
  write_parquet("data-raw/data-dump/gear/gear_fishing_gear.parquet")
tbl_mar(con, "gear.gear_category") |>
  collect() |>
  write_parquet("data-raw/data-dump/gear/gear_gear_category.parquet")
tbl_mar(con, "gear.isscfg") |>
  collect() |>
  write_parquet("data-raw/data-dump/gear/gear_isscfg.parquet")

