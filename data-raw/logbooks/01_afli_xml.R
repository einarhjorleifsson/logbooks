# Extract signal data from afladagb_xml_mottaka XML column
#
# Input:  data-dump/logbooks/afli/afladagb_xml_mottaka.parquet
# Output: data-raw/logbooks/afli/sensor_xml_nmea.parquet
#         data-raw/logbooks/afli/sensor_xml_track.parquet
#
# Bypasses xml2 entirely; uses DuckDB regex on the raw XML strings.
#
# Construct 1 — NMEA: GPS_DATA sentences keyed to submission/VISIR;
#   passed to orit::hr_message_parse() in R for coordinate parsing.
#   Two sub-queries: root-level GPS_DATA (visir = NULL) and per-FAERSLA
#   GPS_DATA (tied to VISIR).
#
# Construct 2 — Track: SJALFVIRKIR_MAELAR LOG entries via two-level unnest
#   (submission → FAERSLA block → LOG entry); all fields extracted in SQL.
#
# Linking keys in both outputs:
#   id    — submission row id
#   vid   — vessel id (skip_nr)
#   visir — station id (NA for root-level GPS_DATA)

library(arrow)
library(duckdb)
library(dplyr)
library(purrr)
library(lubridate)
library(orit)
library(stringr)

ORIT_TYPES <- orit::hr_types$type[!is.na(orit::hr_types$type)]

SRC       <- "data-dump/logbooks/afli/afladagb_xml_mottaka.parquet"
OUT_NMEA  <- "data-raw/logbooks/afli/sensor_xml_nmea.parquet"
OUT_TRACK <- "data-raw/logbooks/afli/sensor_xml_track.parquet"


# Parse NMEA strings via orit --------------------------------------------------

parse_nmea <- function(nmea_raw) {
  nmea_raw <- nmea_raw |>
    mutate(nmea_type = str_extract(m, "(?<=\\$[A-Z]{2})[A-Z]{3}")) |>
    filter(nmea_type %in% ORIT_TYPES)

  map(unique(nmea_raw$nmea_type), \(tp) {
    input <- nmea_raw |>
      filter(nmea_type == tp) |>
      transmute(
        id           = id,
        message_type = tp,
        json_message = NA_character_,
        type         = tp,
        m            = m,
        vid, visir, date_hint, source
      )
    tryCatch(hr_message_parse(input, TYPE = tp), error = \(e) NULL)
  }) |>
    compact() |>
    bind_rows() |>
    mutate(
      timi = case_when(
        !is.na(datestamp) ~
          as_datetime(datestamp) + dseconds(period_to_seconds(timestamp)),
        !is.na(timestamp) & !is.na(date_hint) ~
          as_datetime(ymd(date_hint)) + dseconds(period_to_seconds(timestamp)),
        .default = NA_POSIXct_
      )
    ) |>
    select(
      id, vid, visir, source, nmea_type = type,
      timi, lat, lon,
      speed_kn   = any_of("spd_over_grnd"),
      course_deg = any_of("true_course")
    )
}


# Run --------------------------------------------------------------------------

con <- dbConnect(duckdb())
dbExecute(con, sprintf("CREATE VIEW src AS SELECT * FROM read_parquet('%s')", SRC))


# Construct 1: NMEA ------------------------------------------------------------

nmea_raw <- dbGetQuery(con, r"-(
  WITH
  faersla AS (
    SELECT
      id,
      skip_nr            AS vid,
      fra::TIMESTAMP::DATE::VARCHAR AS fra,
      unnest(regexp_extract_all(sending, '<FAERSLA>([\s\S]*?)</FAERSLA>', 1)) AS fb
    FROM src
    WHERE sending LIKE '%GPS_DATA%'
  ),
  per_faersla AS (
    SELECT
      id, vid,
      nullif(regexp_extract(fb, '<VISIR>(\d+)</VISIR>', 1), '')::INTEGER AS visir,
      coalesce(
        nullif(regexp_extract(fb, '<VEDAGS>([^<]+)</VEDAGS>', 1), ''),
        fra
      )                                                           AS date_hint,
      'faersla_gps'                                               AS source,
      trim(regexp_extract(fb, '<GPS_DATA>([^<]+)</GPS_DATA>', 1)) AS m
    FROM faersla
    WHERE fb LIKE '%GPS_DATA%'
  ),
  root_gps AS (
    SELECT
      id,
      skip_nr            AS vid,
      NULL::INTEGER      AS visir,
      fra::TIMESTAMP::DATE::VARCHAR AS date_hint,
      'root_gps'         AS source,
      trim(regexp_extract(
        regexp_extract(sending, '^([\s\S]*?)<FAERSLA', 1),
        '<GPS_DATA>([^<]+)</GPS_DATA>', 1
      ))                 AS m
    FROM src
    WHERE sending LIKE '%GPS_DATA%'
  )
  SELECT * FROM per_faersla WHERE m != ''
  UNION ALL
  SELECT * FROM root_gps   WHERE m IS NOT NULL AND m != ''
)-")

sensor_xml_nmea <- nmea_raw |> parse_nmea()
write_parquet(sensor_xml_nmea, OUT_NMEA)


# Construct 2: Track -----------------------------------------------------------

sensor_xml_track <- dbGetQuery(con, r"-(
  WITH
  faersla AS (
    SELECT
      id,
      skip_nr AS vid,
      unnest(regexp_extract_all(sending, '<FAERSLA>([\s\S]*?)</FAERSLA>', 1)) AS fb
    FROM src
    WHERE sending LIKE '%SJALFVIRKIR_MAELAR%'
  ),
  logs AS (
    SELECT
      id, vid,
      nullif(regexp_extract(fb, '<VISIR>(\d+)</VISIR>', 1), '')::INTEGER    AS visir,
      unnest(regexp_extract_all(fb, '<LOG>([\s\S]*?)</LOG>', 1))            AS lb
    FROM faersla
    WHERE fb LIKE '%SJALFVIRKIR_MAELAR%'
  )
  SELECT
    id, vid, visir,
    nullif(regexp_extract(lb, '<TIMI>([^<]+)</TIMI>',                1), '')::TIMESTAMP AS timi,
    nullif(regexp_extract(lb, 'BREIDD="([^"]+)"',                    1), '')::DOUBLE    AS lat,
    nullif(regexp_extract(lb, 'LENGD="([^"]+)"',                     1), '')::DOUBLE    AS lon,
    nullif(regexp_extract(lb, '<SKIP_HRADI>([^<]+)</SKIP_HRADI>',    1), '')::DOUBLE    AS speed,
    nullif(regexp_extract(lb, '<SKIP_STEFNA>([^<]+)</SKIP_STEFNA>',  1), '')::DOUBLE    AS heading
  FROM logs
)-")

write_parquet(sensor_xml_track, OUT_TRACK)

dbDisconnect(con)
