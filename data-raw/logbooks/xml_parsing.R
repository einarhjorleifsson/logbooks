library(orit)
library(arrow)
library(xml2)
library(tidyverse)

# ORIT_TYPES: character vector of NMEA sentence types orit can parse (e.g. "GGA", "RMC", ...)
ORIT_TYPES <- orit::hr_types$type
ORIT_TYPES <- ORIT_TYPES[!is.na(ORIT_TYPES)]
# NOTE: orit::hr_message_parse() parses one NMEA string once found

# Load data -----------------------------------------------------------------------

d <- read_parquet("data-dump/logbooks/afli/afladagb_xml_mottaka.parquet")

# Each row in d is a batch submission (~monthly) from one vessel.
# Columns: id, ret_val, gerd, skip_nr, fra, til, londun_faersla, email_id, hugbunadur, sending
#
# Structure inside d$sending (XML):
#   AFLADAGBOKARSENDING          (root — one per row)
#     GPS_DATA                   (single NMEA fix at submission time; no VISIR)
#     TIMAB_FRA                  (submission timestamp)
#     FAERSLA                    (one per station/haul; 70–140 per batch)
#       VISIR                    (station key — links to stofn/afli)
#       GPS_DATA                 (single NMEA fix for this station)
#       STOFN                    (station metadata: gear, date, depth, harbour, ...)
#         VEDAGS                 (date of haul)
#         POS  [LAT, LON]        (start position as attributes — DDMM integers)
#         POS_END [LAT, LON]     (end position)
#       LINA / DRAGNOT / ...     (gear-specific effort)
#       AFLI                     (catch, one per species)
#       SJALFVIRKIR_MAELAR       (automatic logger — present only on equipped vessels)
#         LOG                    (one row per fix, ~10 min intervals)
#           TIMI                 (datetime)
#           STADSETNING [BREIDD, LENGD]  (decimal degrees, signed, use as-is)
#           SKIP_HRADI           (speed)
#           SKIP_STEFNA          (heading)

# Parse all XML up front (slow — ~5 min for full 483k rows; keep for session reuse)
# Parse all XML up front — only needed for bulk scanning (slow: ~5 min / 483k rows)
# m <- map(d$sending, read_xml)

# Known rich examples -------------------------------------------------------------
# Selected from a 20k-row subsample; chosen for high SJALFVIRKIR_MAELAR/LOG count.
#
#   row     id   skip_nr  fra          span_hrs  n_faersla  n_log
#   140340  140618  2184  2012-09-04    600         86       5522
#   221031  221951  2750  2014-07-01    624         82       5421
#   276446  278352  2750  2015-11-01    528         68       4949
#   471281  474111  2184  2020-04-02    768        132       4859
#    96825   94747  2184  2011-09-11    816        144       4691
#   120868  121711  2184  2012-03-19    792         99       4499
#
# Parse just one row directly from d$sending — no need to parse all 483k:
x <- read_xml(d$sending[[140340]])   # vessel 2184, Sep 2012, 86 FAERSLAs, 5522 LOG fixes

# Tree overview -------------------------------------------------------------------

xml_structure(x)         # full document tree — good first look at any XML
xml_structure(x, 2)      # depth-limited view (2 levels); xml2 >= 1.3.4

# xml2 navigation -----------------------------------------------------------------

# xml_find_first()  — one xml_node  (xml_missing if absent, never errors)
# xml_find_all()    — xml_nodeset   (length 0 if absent)
#
# XPath:
#   "CHILD"      direct child
#   "A/B"        A → its child B
#   ".//CHILD"   any descendant (anywhere below current node)

# All FAERSLA nodes in the document
faerslur <- xml_find_all(x, ".//FAERSLA")
length(faerslur)

f1 <- faerslur[[1]]   # first station

# Reading content
xml_text(xml_find_first(f1, "VISIR"))
xml_text(xml_find_first(f1, "GPS_DATA"))
xml_text(xml_find_first(f1, "STOFN/VEDAGS"))   # nested: STOFN → child VEDAGS

# Attributes: <POS LAT="6541.6" LON="-2312.4"/>
pos <- xml_find_first(f1, "STOFN/POS")
xml_attr(pos, "LAT")
xml_attr(pos, "LON")
xml_attrs(pos)              # all attributes as named character vector

# Children of a node
xml_children(f1)            # xml_nodeset of direct children
xml_name(xml_children(f1))  # just tag names

# Iterate: VISIR + GPS string from every FAERSLA
map(faerslur, \(f) list(
  visir   = xml_text(xml_find_first(f, "VISIR")),
  gps_raw = xml_text(xml_find_first(f, "GPS_DATA"))
)) |> head(5)

# Continuous track (SJALFVIRKIR_MAELAR) ------------------------------------------

# Count LOG entries per FAERSLA in this document
map_int(faerslur, \(f) length(xml_find_all(f, ".//SJALFVIRKIR_MAELAR/LOG"))) |>
  table()

# Extract the full track from one FAERSLA
logs <- xml_find_all(f1, ".//SJALFVIRKIR_MAELAR/LOG")

track <- map(logs, \(log) {
  stad <- xml_find_first(log, "STADSETNING")
  tibble(
    timi    = ymd_hms(xml_text(xml_find_first(log, "TIMI"))),
    lat     = as.numeric(xml_attr(stad, "BREIDD")),
    lon     = as.numeric(xml_attr(stad, "LENGD")),
    speed   = as.numeric(xml_text(xml_find_first(log, "SKIP_HRADI"))),
    heading = as.numeric(xml_text(xml_find_first(log, "SKIP_STEFNA")))
  )
}) |> list_rbind()
track

# Extract the full track from ALL FAERSLAs in the document (keyed by VISIR)
track_all <- map(faerslur, \(f) {
  visir <- as.integer(xml_text(xml_find_first(f, "VISIR")))
  map(xml_find_all(f, ".//SJALFVIRKIR_MAELAR/LOG"), \(log) {
    stad <- xml_find_first(log, "STADSETNING")
    tibble(
      visir   = visir,
      timi    = ymd_hms(xml_text(xml_find_first(log, "TIMI"))),
      lat     = as.numeric(xml_attr(stad, "BREIDD")),
      lon     = as.numeric(xml_attr(stad, "LENGD")),
      speed   = as.numeric(xml_text(xml_find_first(log, "SKIP_HRADI"))),
      heading = as.numeric(xml_text(xml_find_first(log, "SKIP_STEFNA")))
    )
  }) |> list_rbind()
}) |> list_rbind()
track_all
# Coordinates are decimal degrees, already signed — use as-is.
# Contrast with rafr_sjalfvirkir_maelar in the Oracle dump, which has
# wacky DDMM encoding and requires coordinate recovery.

# Searching for NMEA strings ------------------------------------------------------

# NMEA sentences start with "$<talker><type>", e.g. "$GPGGA,..."
# talker: GP (GPS), GN (GNSS), II (integrated), ...
# type:   3-letter code matching ORIT_TYPES

# Collect all text content in the document (one string per tag)
all_text <- xml_text(xml_find_all(x, "//*"))

# Regex over known ORIT types
nmea_pattern <- paste0("\\$(GP|GN|II)(", paste(ORIT_TYPES, collapse = "|"), ")")
nmea_hits    <- all_text[grepl(nmea_pattern, all_text)]
nmea_hits

# Parse each NMEA hit with orit
map(nmea_hits, \(s) tryCatch(orit::hr_message_parse(s), error = \(e) NULL)) |>
  compact() |>
  list_rbind()
