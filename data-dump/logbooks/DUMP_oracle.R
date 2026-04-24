# messy flow - should be reconstructed
# note, run on hafdruna / hafbjarmi, the files cp
library(mar)
library(tidyverse)
library(duckdbfs)
con <- connect_mar()
dump <- function(overview, owner) {

  for(i in 1:nrow(overview)) {
    tbl <- paste0(overview$owner[i], ".", overview$name[i])
    print(tbl)
    out <- paste0("tmp_agf/",
                  owner,
                  "/",
                  overview$name[i], ".parquet")
    result <- tryCatch(
      {
        tbl_mar(con, tbl)
      },
      error = function(e) {
        message("Error querying table: ", e$message)
        NULL  # Return NULL on error
      }
    )

    # Check if result is NULL before using it
    if (!is.null(result)) {
      # Continue with operations on result
      d <-
        result |>
        collect()
      if(nrow(d) > 0) {
        d |> duckdbfs::write_dataset(out)
      }
    } else {
      message("Skipping table operations due to error")
    }

  }
}
get_tables <- function(owner) {
  mar_tables(con, owner) |> collect() |>
    rename(name = table_name)
}
get_views <- function(owner) {
  mar_views(con, owner) |> collect() |>
    rename(name = view_name)
}

owners <- c("agf")
for(i in 1:length(owners)) {
  print(owners[i])
  # tables
  ov <- owners[i] |> get_tables()
  ov |> write_dataset(paste0("tmp_agf/", owners[i], "_overview_tables.parquet"))
  ov |> dump(owners[i])
  # views
  ov <- owners[i] |> get_views()
  if(nrow(ov) > 0) {
    ov <- ov[ov$name != "aflagrunnur_sec_v", ]
    ov <- ov[ov$name != "aflagrunnur_th_igildi_v", ]
    ov |> write_dataset(paste0("tmp_agf/", owners[i], "_overview_views.parquet"))
    ov |> dump(owners[i])
  }
}



tbl_mar(con, "kvoti.lods_oslaegt") |>
  collect() |>
  write_parquet("tmp_agf/kvoti_lods_oslaegt.parquet")
