# Can not turn visir to integer because some 25 records too long
library(mar)
library(tidyverse)
library(duckdbfs)
con <- connect_mar()
tbl_mar(con, "afli.stofn") |>
  collect() |>
  mutate(vedags = as_date(vedags),
         ldags = as_date(ldags)) |>
  write_dataset("data-raw/data-dump/logbooks-afli/afli_stofn.parquet")
tbl_mar(con, "afli.toga") |>
  collect() |>
  write_dataset("data-raw/data-dump/logbooks-afli/afli_toga.parquet")
tbl_mar(con, "afli.lineha") |>
  collect() |>
  write_dataset("data-raw/data-dump/logbooks-afli/afli_lineha.parquet")
tbl_mar(con, "afli.gildra") |>
  collect() |>
  write_dataset("data-raw/data-dump/logbooks-afli/afli_gildra.parquet")
tbl_mar(con, "afli.hringn") |>
  collect() |>
  write_dataset("data-raw/data-dump/logbooks-afli/afli_hringn.parquet")
tbl_mar(con, "afli.afli") |>
  collect() |>
  write_dataset("data-raw/data-dump/logbooks-afli/afli_afli.parquet")
tbl_mar(con,'afli.grasl_stofn') |>
  collect() |>
  write_dataset("data-raw/data-dump/logbooks-afli/afli_grasl_stofn.parquet")
tbl_mar(con,'afli.grasl_sokn') |>
  collect() |>
  write_dataset("data-raw/data-dump/logbooks-afli/afli_grasl_sokn.parquet")
