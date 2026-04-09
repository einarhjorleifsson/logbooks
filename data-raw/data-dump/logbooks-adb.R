library(mar)
library(tidyverse)
library(duckdbfs)
con <- connect_mar()
# tables <- mar_tables(con, "adb") |> collect()
views <- mar_views(con, "adb") |> collect() |>
  mutate(tbl = paste0(owner, ".", view_name))
for(i in 1:nrow(views)) {
  tbl <- views$tbl[i]
  fil <- paste0(views$view_name[i], ".parquet")
  d <-
    tbl_mar(con, tbl) |>
    collect()
  if(nrow(d) > 0) {
    d |> duckdbfs::write_dataset(paste0("data-raw/data-dump/logbooks-adb/adb_", fil))
  }
}


