library(nanoparquet)
library(tidyverse)
library(omar)
con <- connect_mar()


tbl_mar(con, "asfis.asfis") |>
  collect() |>
  write_parquet("data-raw/data-dump/species/asfis.parquet")
