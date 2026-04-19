library(mar)
library(tidyverse)
library(duckdbfs)
library(nanoparquet)
library(xml2)
con <- connect_mar()


# tbl_mar(con, "afli.ra_email_skeyti") |> collect() -> d
# # creates an error d |> write_parquet("testo.parquet")

OUT_DIR <- "data-raw/data-dump/afli"
SCHEMA <- "afli"
get_tables_and_views <- function(con, owner) {
  bind_rows(mar_tables(con, owner) |> collect() |>
              rename(name = table_name) |>
              mutate(type = "table"),
            mar_views(con, owner) |> collect() |>
              rename(name = view_name) |>
              mutate(type = "view"))
}
get_fields <- function(schema.table) {
  mar_fields(con, schema.table) |>
    collect() |>
    rename(table = table_name,
           column = column_name)
}


ov <- get_tables_and_views(con, SCHEMA)
ov |> write_parquet(paste0(OUT_DIR, "/", "_overview_tables.parquet"))

ov |>
  unite(owner, name, col = name, sep = ".") |>
  pull(name) |>
  purrr::map_df(get_fields) |>
  write_parquet(paste0(OUT_DIR, "/", "_overview_fields.parquet"))

for(i in 1:nrow(ov)) {
  table <- paste0(ov$owner[i], ".", ov$name[i])
  result <-
    try(tbl_mar(con, table) |> collect(),
        silent = FALSE)

  if (class(result)[1] == "try-error") {
    print(paste0("An error reading:", table))
    #print(geterrmessage())
  } else {
    print(table)
    if(!table %in% "afli.ra_email_skeyti") {
      result |>
        write_parquet(paste0(OUT_DIR,
                             "/",
                             str_remove(table,
                                        paste0(SCHEMA, ".")),
                             ".parquet"))
    }
  }
}


# XML special case: AFLADAGB_XML_MOTTAKA
result <- dbGetQuery(con,
                     'SELECT xmlserialize(DOCUMENT SENDING AS CLOB indent size = 2) AS SENDING
   FROM \"AFLI\".\"AFLADAGB_XML_MOTTAKA\"')

afladagb_xml_mottaka <-
  dplyr::tbl(con, sql('SELECT
                      ID,
                      RET_VAL,
                      GERD,
                      SKIP_NR,
                      FRA,
                      TIL,
                      LONDUN_FAERSLA,
                      EMAIL_ID,
                      HUGBUNADUR
                      FROM "AFLI"."AFLADAGB_XML_MOTTAKA"')) |>
  collect()
names(afladagb_xml_mottaka) <- names(afladagb_xml_mottaka) |> tolower()
afladagb_xml_mottaka$sending <- result$SENDING
afladagb_xml_mottaka |> write_parquet("dump/afladagb_xml_mottaka.parquet")

