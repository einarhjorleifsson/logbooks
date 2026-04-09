library(mar)
library(tidyverse)
library(arrow)
con <- connect_mar()
# tables <- mar_tables(con, "adb") |> collect()
tables <- mar_tables(con, "fs_afladagbok") |> collect() |>
  mutate(tbl = paste0(owner, ".", table_name))
for(i in 1:nrow(tables)) {
  tbl <- tables$tbl[i]
  fil <- paste0(tables$table_name[i], ".parquet")
  # drop a file with a json-string
  if(i != 4) {
  d <-
    tbl_mar(con, tbl) |>
    collect()
  if(nrow(d) > 0) {
    d |> duckdbfs::write_dataset(paste0("data-raw/data-dump/logbooks-fsafladagbok/", fil))
  }
  }
}


