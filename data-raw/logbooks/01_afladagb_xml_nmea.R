# Extract all NMEA sentences from afladagb_xml_mottaka
#
# Input:  data-raw/data-dump/afli/afladagb_xml_mottaka.parquet
# Output: data/afli/nmea.parquet
#
# Approach:
#   Step 1 – XML → flat NMEA table
#     One row per sentence; only types recognised by orit are kept.
#     Sentences live in GPS_DATA nodes at two levels of the XML tree:
#       /AFLADAGBOKARSENDING/GPS_DATA  → root, not tied to a station (visir = NA)
#       .//FAERSLA/GPS_DATA            → per-station, keyed to VISIR
#     A text-node scan across 1000+ rows covering all software variants
#     (Trackwell, SeaData, NA) confirmed no other node names carry NMEA.
#
#   Step 2 – Parse via orit::hr_message_parse(), grouped by NMEA type
#     Metadata columns (vid, visir, date_hint, source) are placed after the
#     four required mock hafriti columns so they pass through unchanged.
#
# Linking keys in output:
#   id         submission row id from d
#   vid        vessel id (d$skip_nr)
#   visir      station id; NA for root-level GPS_DATA
#
# timi notes:
#   RMC  – self-contained (datestamp + timestamp in sentence)
#   GGA  – time-only timestamp anchored to date_hint
#   GLL  – time-only timestamp anchored to date_hint when present;
#           older short-format GLL sentences carry no timestamp → timi = NA

library(arrow)
library(xml2)
library(purrr)
library(dplyr)
library(stringr)
library(lubridate)
library(orit)

ORIT_TYPES <- orit::hr_types$type   # all NMEA types orit can parse
i <- !is.na(ORIT_TYPES)
ORIT_TYPES <- ORIT_TYPES[i]
any(i)

for(i in 1:length(ORIT_TYPES)) {
  i <- grepl("GGA", d$sending)
  if(any(i)) print(ORIT_TYPES[i])
}

# ------------------------------------------------------------------------------
# Step 1 — extract_nmea(): one XML submission → flat NMEA rows
# ------------------------------------------------------------------------------

extract_nmea <- function(id, vid, fra, xml_str) {
  doc <- tryCatch(read_xml(xml_str), error = \(e) NULL)
  if (is.null(doc)) return(NULL)

  rows <- list()

  # Root GPS_DATA — not tied to a station
  root_raw <- trimws(xml_text(xml_find_first(doc, "/AFLADAGBOKARSENDING/GPS_DATA")))
  if (nchar(root_raw) > 0)
    rows[[length(rows) + 1]] <- tibble(
      id        = id,
      vid       = vid,
      visir     = NA_real_,
      date_hint = as.character(as_date(fra)),   # date from submission timestamp
      source    = "root_gps",
      m         = root_raw
    )

  # FAERSLA GPS_DATA — keyed to VISIR
  for (f in xml_find_all(doc, ".//FAERSLA")) {
    raw <- trimws(xml_text(xml_find_first(f, "GPS_DATA")))
    if (length(raw) == 0 || is.na(raw) || nchar(raw) == 0) next
    rows[[length(rows) + 1]] <- tibble(
      id        = id,
      vid       = vid,
      visir     = suppressWarnings(as.numeric(xml_text(xml_find_first(f, "VISIR")))),
      date_hint = trimws(xml_text(xml_find_first(f, "STOFN/VEDAGS"))),
      source    = "faersla_gps",
      m         = raw
    )
  }

  if (length(rows) == 0) return(NULL)

  list_rbind(rows) |>
    mutate(nmea_type = str_extract(m, "(?<=\\$[A-Z]{2})[A-Z]{3}")) |>
    filter(nmea_type %in% ORIT_TYPES)
}

# ------------------------------------------------------------------------------
# Step 2 — parse one NMEA type via orit::hr_message_parse()
# ------------------------------------------------------------------------------

parse_nmea_type <- function(df, type) {
  input <- df |>
    filter(nmea_type == type) |>
    transmute(
      # mock hafriti columns — required by hr_message_parse; order matters
      id           = id,
      message_type = type,
      json_message = NA_character_,
      type         = type,
      m            = m,
      # metadata carried through (must come after the mock block)
      vid, visir, date_hint, source
    )
  if (nrow(input) == 0) return(NULL)
  tryCatch(hr_message_parse(input, TYPE = type), error = \(e) NULL)
}

# ------------------------------------------------------------------------------
# Run
# ------------------------------------------------------------------------------

d <- read_parquet("data-raw/data-dump/afli/afladagb_xml_mottaka.parquet")

# -- Step 1: extract -----------------------------------------------------------
nmea_raw <- pmap(
  list(id = d$id, vid = d$skip_nr, fra = d$fra, xml_str = d$sending),
  extract_nmea
) |> list_rbind()

# -- Step 2: parse, unify, and assemble ----------------------------------------
nmea <- map(unique(nmea_raw$nmea_type), \(tp) parse_nmea_type(nmea_raw, tp)) |>
  compact() |>
  bind_rows() |>
  mutate(
    timi = case_when(
      # RMC: sentence carries its own date
      !is.na(datestamp) ~
        as_datetime(datestamp) + dseconds(period_to_seconds(timestamp)),
      # GGA / GLL (full format): anchor time-only timestamp to date_hint
      !is.na(timestamp) & !is.na(date_hint) ~
        as_datetime(ymd(date_hint)) + dseconds(period_to_seconds(timestamp)),
      .default = NA_POSIXct_
    )
  ) |>
  select(
    id, vid, visir, source, nmea_type = type,
    timi, lat, lon,
    spd_over_grnd = any_of("spd_over_grnd"),
    true_course   = any_of("true_course")
  )

# -- Write ---------------------------------------------------------------------
dir.create("data/afli", recursive = TRUE, showWarnings = FALSE)
write_parquet(nmea, "data/afli/nmea.parquet")
