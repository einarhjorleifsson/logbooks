library(tidyverse)
library(rvest)
library(countrycode)

# Country codes ISO_3166 -------------------------------------------------------
page <- read_html("https://en.wikipedia.org/wiki/List_of_ISO_3166_country_codes")
# Extract clean country names from <a> link text (avoids CSS contamination)
names_clean <- page |>
  html_element("table.wikitable") |>
  html_elements("tr") |>
  _[-(1:2)] |>
  map_chr(\(row) {
    first_td <- html_elements(row, "td")[1]
    if (length(first_td) == 0) return(NA_character_)
    links <- html_elements(first_td, "a")
    if (length(links) == 0) return(html_text2(first_td))
    txts <- html_text2(links)
    if (length(txts) == 1) txts
    else paste(txts[-length(txts)], collapse = ", ") |>
      paste(txts[length(txts)], sep = " and ")
  })
iso3166 <- page |>
  html_element("table.wikitable") |>
  html_table() |>
  setNames(c("name", "sovereignty", "alpha_2", "alpha_3", "numeric", "iso3166_2", "tld")) |>
  slice(-(1:2)) |>
  mutate(
    name    = names_clean[-1],   # drop extra header row to align
    name = str_remove(name, " and .*$"),
    name = ifelse(name == ", Taiwan (Province of China)",
                  "Taiwan (Province of China)",
                  name),
    alpha_2 = str_extract(alpha_2, "\\b[A-Z]{2}\\b")
  ) |>
  filter(!is.na(alpha_2))
iso3166 |> nanoparquet::write_parquet("data/aux/country_codes_ISO3166.parquet")
# MMSI country code ------------------------------------------------------------
maritime_identification_digits <-
  "https://en.wikipedia.org/wiki/Maritime_identification_digits" |>
  read_html() %>%
  #html_nodes(xpath='//*[@id="mw-content-text"]/table[1]') %>%
  html_table() |>
  bind_rows() |>
  janitor::clean_names() |>
  rename(MID = codes)
maritime_identification_digits <-
  maritime_identification_digits |>
  mutate(MID = str_remove(MID, "\\(218 from former German Democratic Republic\\)")) |>
  separate(MID, into = paste0("c", 1:20), extra = "drop", sep = ";") |>
  gather(dummy, MID, -country) |>
  select(-dummy) |>
  mutate(MID = str_trim(MID)) |>
  drop_na()
maritime_identification_digits |>
  nanoparquet::write_parquet("data/aux/maritime_identification_digits.parquet")


## Call signs and country ------------------------------------------------------
if(FALSE) {
  library(mar)
  con <- connect_mar()
  tbl_mar(con, "ops$einarhj.vessel_cs_itu_prefix") |>
    collect() |>
    nanoparquet::write_parquet("data/aux/callsign_prefix.parquet")
}


