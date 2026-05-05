# ASTD typeB ZIP → Hive-partitioned Parquet
#
# Input:  data-raw/ais/astdB/data-raw/ais/astdB/astd_classb_2014-01-01_2024-12-31.zip
# Output: data/ais/astdB/year=YYYY/month=MM/data_0.parquet
#
# Run headless:
#   nohup R < data-raw/ais/DATASET_astdB_zip-to-parquet.R --vanilla \
#     > data-raw/ais/log/astdB_$(date +%F).log 2>&1 &

library(tidyverse)
library(duckdbfs)


tmpdir <- tempdir()
unzip("data-dump/ais/astdB/astd_classb_2014-01-01_2024-12-31.zip", exdir = tmpdir)

directory <-
  tibble(file = dir(tmpdir, recursive = TRUE, full.names = TRUE, pattern = ".gz")) |>
  mutate(year = stringr::str_extract(file, "(?<=date_utc=)\\d{4}"),
         month = stringr::str_extract(file, "(?<=date_utc=\\d{4}-)\\d{2}"),
         .before = file) |>
  filter_out(file, str_detect("date_utc=2019-02-12")) |>
  filter_out(file, str_detect("date_utc=2021-06-12"))

years <- unique(directory$year)
# error in some files
for(y in years) {
  print(y)
  for(m in 1:12) {
    directory |> filter(year == y, month == m) |> pull(file) |>
      read_csv(
        col_types = list(
          dsrc = col_character(),
          ais_class = col_character(),
          cc_iso3 = col_character(),
          mmsi = col_integer(),
          maneuvre = col_double(),
          lon = col_double(),
          lat = col_double(),
          status = col_double(),
          rot = col_double(),
          cod = col_double(),
          true_heading = col_double(),
          length = col_double(),
          breadth = col_double(),
          draught = col_double(),
          date_time_utc = col_datetime())) |>
      mutate(mmsi = as.integer(mmsi),
             month = as.integer(month(date_time_utc)),
             year = as.integer(year(date_time_utc))) |>
      duckdbfs::write_dataset("data-raw/ais/astdB",partitioning = c("year", "month"), format = "parquet")
  }
}

