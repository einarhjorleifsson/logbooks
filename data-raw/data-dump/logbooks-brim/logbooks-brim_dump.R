library(duckdbfs)
library(tidyverse)
con <- connect_mar(dbname = "BRIM")
# tables <- mar_tables(con, "adb") |> collect()
tables <- mar_tables(con, "logbook") |> collect() |>
  mutate(tbl = paste0(owner, ".", table_name))
for(i in 1:nrow(tables)) {
  tbl <- tables$tbl[i]
  fil <- paste0(tables$table_name[i], ".parquet")
  d <-
    tbl_mar(con, tbl) |>
    collect()
  if(nrow(d) > 0) {
    d |> duckdbfs::write_dataset(paste0("data-raw/data-dump/logbooks-brim/logbooks_", fil))
  }
}

