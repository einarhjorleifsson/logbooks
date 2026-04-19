# Create standard summary tables
# Input: data-raw/data-dump/fs_afladagbok/*.parquet
# Outpt: data/fs_afladagbok/*.parquet


# Seeding ----------------------------------------------------------------------

library(whack) # pak::pak("einarhjorleifsson/whack")
library(geo)
library(tidyverse)
library(nanoparquet)

SCHEMA <- "fs_afladagbok"
dictionary <- read_parquet("data/dictionary.parquet") |>
  filter(schema == SCHEMA)
gear_mapping <- read_parquet("data/gear/gear_mapping.parquet")

# Trips ------------------------------------------------------------------------
trip <- read_parquet("data-raw/data-dump/fs_afladagbok/ws_veidiferd.parquet") |>
  wk_translate(dictionary) |>
  rename(.tid = id) |>
  select(-c(skip_nafn, kallmerki, umdaemisnumer, skip_farsimi, utgerd_kt, snt)) |>
  mutate(schema = SCHEMA) |>
  select(vid, T1, hid1, T2, hid2, .tid, source, schema, everything())

# Station ----------------------------------------------------------------------
## Strictly speaking a typical logbook station table is actually a combination
## of a station table and a sample sample table, but by historical convention
## have been treated as one. If one were to follow the grammar of fisheries
## data one would have a station containing broad information (date, geograhic
## location, etc and then a sample table that contains the detail of the gear
## deployment (lon1, lat1, ... and event times (t0, t1, t2, ...).

station <-
  read_parquet("data-raw/data-dump/fs_afladagbok/ws_veidi.parquet") |>
  wk_translate(dictionary) |>
  rename(.sid = id) |>
  select(-c(breytt, skraningartimi, snt)) |>
  left_join(trip |> select(.tid, source)) |>
  mutate(schema = SCHEMA,
         date = as_date(coalesce(t0, t1, t2))) |>
  dplyr::select(.tid, .sid, gid, date,
                # fishing event times
                t0, t1, t2,
                # geographic location
                x1, y1, x2, y2,
                # depth
                z1, z2,
                schema, source,
                dplyr::everything())

## Fix coordinate format mess --------------------------------------------------
### Classify coordinate format -------------------------------------------------
### Overview
# Converting raw (x1, y1, x2, y2) to decimal degrees requires two steps:
#   1. Normalise to 6-digit integers  (handled in the conversion block below)
#   2. Apply the correct formula       (driven by the coord_fmt flag added here)
#
# The raw integers use one of two encodings depending on the data source:
#   - DMS  (degrees-minutes-seconds):  DD*10000 + MM*100 + SS,  e.g. 641523 = 64°15'23"
#   - DDM  (degrees-decimal-minutes):  DD*10000 + MM.CC*100,    e.g. 641523 = 64°15.23'
#
### Classification logic
# Two complementary rules are combined:
#
#   Rule 1 — row-level signal (sufficient, not necessary):
#     After normalising to 6 digits, the last 2 digits represent either seconds
#     (DMS, range 0–59) or hundredths of a minute (DDM, range 0–99).
#     A value >= 60 is therefore impossible in DMS → definitive DDM evidence.
#
#   Rule 2 — source prior (catches ambiguous cases):
#     For known DDM sources the decimal minutes may happen to land below 60,
#     producing no row-level signal even though the format is DDM.
#     These sources are listed in DDM_SOURCES and classified DDM unconditionally.
#
# Both rules are lifted to the trip level (group_by .tid + any()).
# Trips are internally consistent — almost no "mixed" trips exist in the data —
# so a single DDM-signalled row is enough to classify the whole trip as DDM.
# This also handles chronology: if a source changed format at some point, the
# trips after the switch will show the DDM signal without needing a date cutoff.
#
# Sources with partial DDM (e.g. "Jóhann Gíslason raun", ~14% of trips) are
# handled automatically by Rule 1: individual DDM trips fire the signal and get
# classified correctly; the remaining DMS trips default to "dms".
#
# Sources known to encode in DDM
DDM_SOURCES <- c(
  "FRA TAKTIKAL", "FRA TAKTIKAL - GAFL AUTO",
  "Marína raun",  "Marína raun - GAFL AUTO", "Marína raun - Skrifstofa VES",
  "GAFL BÓK UPPFÆRÐ FRÁ TAKTIKAL"
)

station <- station |>
  mutate(
    ny1 = nchar(as.character(abs(y1))),
    # Normalise y1 to 6-digit DDMMCC for format detection only
    y1_det = case_when(
      ny1 == 6 ~ as.numeric(abs(y1)),
      ny1 == 5 ~ abs(y1) * 10,
      ny1 == 4 ~ abs(y1) * 100,
      ny1 == 2 & source == "STOKKUR" ~ abs(y1) * 10000,
      .default = as.numeric(abs(y1))
    ),
    ddm_signal = !is.na(y1_det) & (y1_det %% 100 >= 60)
  ) |>
  group_by(.tid) |>
  mutate(trip_ddm = any(ddm_signal, na.rm = TRUE)) |>
  ungroup() |>
  mutate(
    coord_fmt = case_when(
      trip_ddm | source %in% DDM_SOURCES ~ "ddm",
      .default = "dms"
    )
  ) |>
  select(-ny1, -y1_det, -ddm_signal, -trip_ddm)

### Convert to decimal degrees -------------------------------------------------
station <- station |>
  mutate(
    nx1 = nchar(as.character(abs(x1))),
    nx2 = nchar(as.character(abs(x2))),
    ny1 = nchar(as.character(abs(y1))),
    ny2 = nchar(as.character(abs(y2)))
  ) |>
  # Step 1: normalise all coordinates to 6-digit DDMMSS / DDMMmm
  mutate(
    y1 = case_when(ny1 == 6 ~ y1, ny1 == 5 ~ y1 * 10, ny1 == 4 ~ y1 * 100,
                   ny1 == 2 & source == "STOKKUR" ~ y1 * 10000, .default = y1),
    x1 = case_when(nx1 == 6 ~ x1, nx1 == 5 & gid != 9 ~ x1 * 10,
                   nx1 == 4 & gid != 9 ~ x1 * 100,
                   nx1 == 2 & source == "STOKKUR" ~ x1 * 10000, .default = x1),
    y2 = case_when(ny2 == 6 ~ y2, ny2 == 5 ~ y2 * 10, ny2 == 4 ~ y2 * 100,
                   ny2 == 2 & source == "STOKKUR" ~ y2 * 10000, .default = y2),
    x2 = case_when(nx2 == 6 ~ x2, nx2 == 5 & gid != 9 ~ x2 * 10,
                   nx2 == 4 & gid != 9 ~ x2 * 100,
                   nx2 == 2 & source == "STOKKUR" ~ x2 * 10000, .default = x2)
  ) |>
  # Step 2: fix sign on latitudes (stored as positive in raw data)
  mutate(
    y1 = ifelse(y1 < 0, -y1, y1),
    y2 = ifelse(y2 < 0, -y2, y2)
  ) |>
  # Step 3: convert to decimal degrees using coord_fmt flag
  # validate = FALSE is required because case_when() evaluates every branch
  # across ALL rows before applying conditions. Without it, wk_convert_ddm()
  # would attempt to validate DMS-encoded values (which have minutes >= 60 in
  # their DMS encoding), causing spurious validation errors. Out-of-range
  # results are cleaned up by the range filter in step 5.
  mutate(
    lon1 = case_when(coord_fmt == "ddm" ~ wk_convert_ddm(-x1, validate = FALSE),
                     .default = wk_convert_dms(-x1, validate = FALSE)),
    lat1 = case_when(coord_fmt == "ddm" ~ wk_convert_ddm( y1, validate = FALSE),
                     .default = wk_convert_dms( y1, validate = FALSE)),
    lon2 = case_when(coord_fmt == "ddm" ~ wk_convert_ddm(-x2, validate = FALSE),
                     .default = wk_convert_dms(-x2, validate = FALSE)),
    lat2 = case_when(coord_fmt == "ddm" ~ wk_convert_ddm( y2, validate = FALSE),
                     .default = wk_convert_dms( y2, validate = FALSE))
  ) |>
  # Step 4: fix occasional sign flip on longitude (some sources store longitude
  # as positive; negate all lon > 10 since Iceland is entirely in the western
  # hemisphere; the range filter in step 5 handles any remaining implausible values)
  mutate(
    lon1 = case_when(lon1 >= 10 ~ -lon1, .default = lon1),
    lon2 = case_when(lon2 >= 10 ~ -lon2, .default = lon2)
  ) |>
  # Step 5: range filter — set implausible values to NA
  mutate(
    lon1 = ifelse(between(lon1, -179, 179), lon1, NA_real_),
    lon2 = ifelse(between(lon2, -179, 179), lon2, NA_real_),
    lat1 = ifelse(between(lat1,    0,  89), lat1, NA_real_),
    lat2 = ifelse(between(lat2,    0,  89), lat2, NA_real_)
  ) |>
  select(.tid, .sid, gid, date, t0, t1, t2, lon1, lat1, lon2, lat2,
         z1, z2, schema, source)

station <- station |>
  arrange(date, .sid, t0, t1, t2)

### Add "old" gear codes -------------------------------------------------------
station <- station |>
  left_join(gear_mapping |>
              filter(version == "new") |>
              select(gid, gid_old = map)) |>
  select(.tid, .sid, gid, gid_old, everything())

# Auxillary tables -------------------------------------------------------------
## Each block inner_joins the gear-specific aux table onto base to obtain
## gear code. A final auxillary table is generated downstream and saved.
## Downstream it can be joined to the station table.

## Mobile (dragnót / varpa) ----------------------------------------------------
mobile_aux <-
  read_parquet("data-raw/data-dump/fs_afladagbok/ws_dragnot_varpa.parquet") |>
  wk_translate(dictionary) |>
  select(.sid, n_units, g_mesh, g_width, g_length) |> # g_length refers ot seine "tóglengd"
  inner_join(station |> filter(gid %in% c(1:9, 11)) |> select(.sid), by = ".sid")

## Dredge (plógur) -------------------------------------------------------------
dredge_aux <-
  read_parquet("data-raw/data-dump/fs_afladagbok/ws_plogur.parquet") |>
  wk_translate(dictionary) |>
  select(.sid, n_units, g_mesh, g_width, g_length, g_height) |>
  inner_join(station |> filter(gid == 15) |> select(.sid))

## Static (longline / gillnet / handline) --------------------------------------
static_aux <-
  # question if "moskvi_haed" refers to gear height
  read_parquet("data-raw/data-dump/fs_afladagbok/ws_linanethandf.parquet") |>
  wk_translate(dictionary) |>
  inner_join(station |> select(.sid, gid) |> filter(gid %in% c(1:5, 12, 14, 22)), by = ".sid") |>
  left_join(gear_mapping |> filter(version == "new"), by = "gid") |>
  mutate(n_units =
         case_when(
             gear %in% c("GNS", "GND")  ~ n_nets,
             gear == "LLS"              ~ n_hooks,
             gear == "LHM"              ~ n_jigs),
         g_length =
           case_when(gear %in% c("GNS", "GND") ~ n_units * medal_lengd_neta,
                     .default = NA)) |>
  select(.sid, n_units, g_mesh, g_length, g_height)

## Traps -----------------------------------------------------------------------
trap_aux <-
  read_parquet("data-raw/data-dump/fs_afladagbok/ws_gildra.parquet") |>
  wk_translate(dictionary) |>
  select(.sid, n_units)

## Seine / purse seine ---------------------------------------------------------
seine_aux <-
  read_parquet("data-raw/data-dump/fs_afladagbok/ws_hringn.parquet") |>
  wk_translate(dictionary) |>
  select(.sid,  g_mesh, g_length, g_height)

## Final auxillary table -------------------------------------------------------
auxillary <- bind_rows(mobile_aux, dredge_aux, static_aux, trap_aux, seine_aux)

# Catch ------------------------------------------------------------------------
catch <-
  read_parquet("data-raw/data-dump/fs_afladagbok/ws_afli.parquet") |>
  wk_translate(dictionary) |>
  select(.sid, sid, catch) |>
  group_by(.sid, sid) |>
  summarise(catch = sum(catch, na.rm = TRUE),
            .groups = "drop") |>
  # ensure that catch record not an orphan
  inner_join(station |> select(.sid, schema)) |>
  arrange(.sid, sid)

# Export -----------------------------------------------------------------------
trip    |> write_parquet("data/fs_afladagbok/trip.parquet")
station |> write_parquet("data/fs_afladagbok/station.parquet")
auxillary |> write_parquet("data/fs_afladagbok/auxillary.parquet")
catch   |> write_parquet("data/fs_afladagbok/catch.parquet")
