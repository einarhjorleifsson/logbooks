# Extract navigational / positional data from afladagb_xml_mottaka
#
# Input:  data-raw/data-dump/afli/afladagb_xml_mottaka.parquet
# Output: data/afli/nav.parquet
#
# Three position sources per sending:
#   1. root GPS_DATA       – single NMEA fix per submission (no VISIR)
#   2. FAERSLA GPS_DATA    – single NMEA fix per station
#   3. SJALFVIRKIR_MAELAR  – automatic logger track (multiple fixes per station)
#                            also carries: botndypi, skip_hradi, skip_stefna
#
# Coordinate systems:
#   NMEA (sources 1 & 2)   : DDM format, e.g. "6541.6965" = 65°41.6965′
#                            hemisphere field (N/S/E/W) determines sign
#   SJALFVIRKIR_MAELAR     : decimal degrees, already signed; use as-is
#   STOFN/STADSETNING      : DDMM integers, e.g. 6532 = 65°32′
#                            no hemisphere flag; longitude negated per afli
#                            convention (stored unsigned; Iceland is W)
#                            NOTE: vessels fishing Norwegian/Barents Sea will
#                            have wrong sign from this source

library(arrow)
library(xml2)
library(purrr)
library(dplyr)
library(lubridate)


# Coordinate helpers -----------------------------------------------------------

# NMEA DDM → decimal degrees: "DDMM.mmmm" + hemisphere flag
nmea_ddm_to_dd <- function(nmea, hemi) {
  v   <- suppressWarnings(as.numeric(nmea))
  deg <- trunc(v / 100)
  dd  <- deg + (v - deg * 100) / 60
  ifelse(hemi %in% c("S", "W"), -dd, dd)
}

# STOFN/STADSETNING DDMM integer → decimal degrees
# Longitude negated (afli convention: stored unsigned, West hemisphere assumed)
stofn_breidd_to_dd <- function(x) geo::geoconvert.1(as.numeric(x) * 100)
stofn_lengd_to_dd  <- function(x) -geo::geoconvert.1(as.numeric(x) * 100)

# NMEA sentence parser ---------------------------------------------------------
# Returns one-row tibble: lat, lon, timi, speed_kn, course_deg, nmea_type
# date_hint ("YYYY-MM-DD"): used for GGA/GLL which carry time only, no date
parse_nmea <- function(sentence, date_hint = NA_character_) {
  parts <- strsplit(trimws(sentence), ",")[[1]]
  parts[length(parts)] <- sub("\\*.*", "", parts[length(parts)])
  type  <- sub("^\\$..(.+)$", "\\1", parts[1])

  row <- switch(type,
    GGA = tibble(
      lat        = nmea_ddm_to_dd(parts[3], parts[4]),
      lon        = nmea_ddm_to_dd(parts[5], parts[6]),
      nmea_time  = parts[2],       # HHMMSS[.ss]
      nmea_date  = NA_character_,
      speed_kn   = NA_real_,
      course_deg = NA_real_,
      nmea_type  = "GGA"
    ),
    GLL = tibble(
      lat        = nmea_ddm_to_dd(parts[2], parts[3]),
      lon        = nmea_ddm_to_dd(parts[4], parts[5]),
      nmea_time  = if (length(parts) >= 6) parts[6] else NA_character_,
      nmea_date  = NA_character_,
      speed_kn   = NA_real_,
      course_deg = NA_real_,
      nmea_type  = "GLL"
    ),
    RMC = tibble(
      lat        = nmea_ddm_to_dd(parts[4], parts[5]),
      lon        = nmea_ddm_to_dd(parts[6], parts[7]),
      nmea_time  = parts[2],
      nmea_date  = parts[10],      # DDMMYY
      speed_kn   = suppressWarnings(as.numeric(parts[8])),
      course_deg = suppressWarnings(as.numeric(parts[9])),
      nmea_type  = "RMC"
    ),
    tibble(
      lat = NA_real_, lon = NA_real_, nmea_time = NA_character_,
      nmea_date = NA_character_, speed_kn = NA_real_,
      course_deg = NA_real_, nmea_type = type
    )
  )

  row |> mutate(
    timi = case_when(
      # RMC carries its own date
      !is.na(nmea_date) & nchar(nmea_date) == 6 ~
        dmy_hms(paste(nmea_date,
                      sub("(\\d{2})(\\d{2})(\\d{2}).*", "\\1:\\2:\\3",
                          nmea_time))),
      # GGA/GLL: time only — anchor to date_hint
      !is.na(date_hint) & !is.na(nmea_time) & nchar(nmea_time) >= 6 ~
        ymd_hms(paste(date_hint,
                      sub("(\\d{2})(\\d{2})(\\d{2}).*", "\\1:\\2:\\3",
                          nmea_time))),
      TRUE ~ NA_POSIXct_
    )
  ) |> select(-nmea_time, -nmea_date)
}

# Per-sending extractor --------------------------------------------------------
extract_nav <- function(id, vid, xml_string) {
  doc <- tryCatch(read_xml(xml_string), error = \(e) NULL)
  if (is.null(doc)) return(NULL)

  # -- 1. Root GPS: one fix per sending, not tied to a specific VISIR --------
  root_raw  <- trimws(xml_text(xml_find_first(doc, "/AFLADAGBOKARSENDING/GPS_DATA")))
  root_fra  <- xml_text(xml_find_first(doc, "/AFLADAGBOKARSENDING/TIMAB_FRA"))
  root_date <- tryCatch(
    as.character(as_date(ymd_hms(root_fra))), error = \(e) NA_character_
  )

  root_nav <- if (nchar(root_raw) > 0) {
    tryCatch(
      parse_nmea(root_raw, root_date) |>
        mutate(
          .id         = id,
          vid         = vid,
          visir       = NA_integer_,
          botndypi    = NA_real_,
          skip_hradi  = NA_real_,
          skip_stefna = NA_real_,
          source      = "root_gps"
        ),
      error = \(e) NULL
    )
  } else NULL

  # -- 2 & 3. Per-FAERSLA ----------------------------------------------------
  faerslur <- xml_find_all(doc, ".//FAERSLA")

  faersla_nav <- map(faerslur, \(f) {
    visir  <- suppressWarnings(as.integer(xml_text(xml_find_first(f, "VISIR"))))
    vedags <- trimws(xml_text(xml_find_first(f, "STOFN/VEDAGS")))  # date hint

    # -- 2. FAERSLA GPS: single NMEA fix per station -----------------------
    gps_raw <- trimws(xml_text(xml_find_first(f, "GPS_DATA")))
    faersla_gps <- if (!is.na(gps_raw) && nchar(gps_raw) > 0) {
      tryCatch(
        parse_nmea(gps_raw, vedags) |>
          mutate(
            .id         = id,
            vid         = vid,
            visir       = visir,
            botndypi    = NA_real_,
            skip_hradi  = NA_real_,
            skip_stefna = NA_real_,
            source      = "faersla_gps"
          ),
        error = \(e) NULL
      )
    } else NULL

    # -- 3. SJALFVIRKIR_MAELAR: logger track per station -------------------
    # Coordinates are decimal degrees, already signed; use as-is
    logs <- xml_find_all(f, ".//SJALFVIRKIR_MAELAR/LOG")
    logger_track <- if (length(logs) > 0) {
      map(logs, \(log) {
        stad <- xml_find_first(log, "STADSETNING")
        tibble(
          .id         = id,
          vid         = vid,
          visir       = visir,
          timi        = suppressWarnings(
                          ymd_hms(xml_text(xml_find_first(log, "TIMI")))
                        ),
          lat         = suppressWarnings(as.numeric(xml_attr(stad, "BREIDD"))),
          lon         = suppressWarnings(as.numeric(xml_attr(stad, "LENGD"))),
          botndypi    = suppressWarnings(
                          as.numeric(xml_text(xml_find_first(log, "BOTNDYPI")))
                        ),
          skip_hradi  = suppressWarnings(
                          as.numeric(xml_text(xml_find_first(log, "SKIP_HRADI")))
                        ),
          skip_stefna = suppressWarnings(
                          as.numeric(xml_text(xml_find_first(log, "SKIP_STEFNA")))
                        ),
          speed_kn    = NA_real_,
          course_deg  = NA_real_,
          nmea_type   = NA_character_,
          source      = "sjalfvirkir_maelar"
        )
      }) |> list_rbind()
    } else NULL

    list_rbind(compact(list(faersla_gps, logger_track)))
  }) |> list_rbind()

  bind_rows(root_nav, faersla_nav)
}

# Run --------------------------------------------------------------------------
# Single-threaded (expect ~20–40 min for full 483k rows).
# For parallel: replace pmap() with furrr::future_pmap() after plan(multisession)
library(furrr)
plan(multisession)
d <- read_parquet("data-raw/data-dump/afli/afladagb_xml_mottaka.parquet")

nav <- furrr::future_pmap(
  list(id = d$id, vid = d$skip_nr, xml_string = d$sending),
  extract_nav
) |> list_rbind()



dir.create("data/afli", recursive = TRUE, showWarnings = FALSE)
write_parquet(nav, "data/afli/nav.parquet")
