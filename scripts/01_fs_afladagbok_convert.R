# Create standard summary tables
# Input:  data-raw/data-dump/fs_afladagbok/{ws_veidiferd,ws_veidi,ws_afli,
#           ws_dragnot_varpa,ws_plogur,ws_linanethandf,ws_gildra,ws_hringn}.parquet
# Output: data/fs_afladagbok/{trip,station,fishing_sample,catch}.parquet

# Setup -----------------------------------------------------------------------
library(whack)     # pak::pak("einarhjorleifsson/whack")
library(geo)
library(tidyverse)
library(nanoparquet)

SCHEMA <- "fs_afladagbok"
dictionary   <- read_parquet("data/dictionary.parquet") |> filter(schema == SCHEMA)
gear_mapping <- read_parquet("data/gear/gear_mapping.parquet") |> filter(version == "new")

# Sources known to encode coordinates in DDM (degrees-decimal-minutes).
# All others default to DMS classification via row-level signal.
DDM_SOURCES <- c(
  "FRA TAKTIKAL", "FRA TAKTIKAL - GAFL AUTO",
  "Marína raun",  "Marína raun - GAFL AUTO", "Marína raun - Skrifstofa VES",
  "GAFL BÓK UPPFÆRÐ FRÁ TAKTIKAL"
)

# Trip -------------------------------------------------------------------------
# Built first: `source` (uppruni) is a trip-level column in ws_veidiferd and
# is needed for coordinate classification in ws_veidi below.
trip <-
  read_parquet("data-raw/data-dump/fs_afladagbok/ws_veidiferd.parquet") |>
  wk_translate(dictionary) |>
  rename(.tid = id) |>
  mutate(n_crew = NA_integer_, schema = SCHEMA) |>
  select(.tid, vid, T1, hid1, T2, hid2, n_crew, source, schema)

# Source (ws_veidi) -----------------------------------------------------------
# Translate columns, join trip-level `source` for coordinate classification,
# then convert raw integer coordinates to decimal degrees.
# After this block `source` has: .sid .tid gid t0 t1 t2 lon1 lat1 lon2 lat2
# z1 z2 n_lost date schema.
source <-
  read_parquet("data-raw/data-dump/fs_afladagbok/ws_veidi.parquet") |>
  wk_translate(dictionary) |>
  rename(.sid = id) |>
  select(-c(breytt, skraningartimi, snt, veidarfaeri_efni, athugasemd_txt)) |>
  # uppruni is not in ws_veidi; join from trip to enable DDM_SOURCES classification
  left_join(trip |> select(.tid, source), by = ".tid") |>

  ## Classify coordinate format -------------------------------------------------
  # Rule 1 (row-level): last-2-digits of normalised y1 ≥ 60 is impossible in DMS
  #   → definitive DDM evidence; lifted to trip level.
  # Rule 2 (source prior): known DDM sources classified unconditionally.
  mutate(
    ny1    = nchar(as.character(abs(y1))),
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
  mutate(coord_fmt = case_when(
    trip_ddm | source %in% DDM_SOURCES ~ "ddm",
    .default = "dms"
  )) |>
  select(-ny1, -y1_det, -ddm_signal, -trip_ddm) |>

  ## Convert to decimal degrees -------------------------------------------------
  # Step 1: normalise all coordinates to 6-digit DDMMSS / DDMMmm integers.
  # gid 9 (Flotvarpa) has mixed decimal-degree / DMS encoding that cannot be
  # disentangled without per-source classification — skip normalisation for x.
  mutate(
    nx1 = nchar(as.character(abs(x1))), nx2 = nchar(as.character(abs(x2))),
    ny1 = nchar(as.character(abs(y1))), ny2 = nchar(as.character(abs(y2)))
  ) |>
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
  # Step 2: fix sign on latitudes (stored positive in raw data)
  mutate(y1 = ifelse(y1 < 0, -y1, y1), y2 = ifelse(y2 < 0, -y2, y2)) |>
  # Step 3: convert to decimal degrees.
  # validate = FALSE required: case_when() evaluates all branches before applying
  # conditions, so DMS-encoded values would trigger spurious DDM validation errors.
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
  # Step 4: negate any longitude still positive (some sources store as positive)
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
  mutate(
    date   = as_date(coalesce(t0, t1, t2)),
    schema = SCHEMA
  ) |>
  select(-c(x1, y1, x2, y2, nx1, nx2, ny1, ny2, coord_fmt, source)) |>
  arrange(date, .sid, t0, t1, t2)

# Helper: station keys + ICES gear labels for inner-joining aux tables
source_gear <- source |>
  select(.sid, gid) |>
  left_join(gear_mapping |> select(gid, gear), by = "gid")

# Station (spatial / temporal envelope) ---------------------------------------
# Narrow table: keys, date, positions, depth. Fishing operation detail is in
# fishing_sample; the two join on .sid.
station <-
  source |>
  select(.sid, .tid, date, lon1, lat1, lon2, lat2, z1, z2, schema)

# Aux tables ------------------------------------------------------------------
# Each block reads one gear-specific parquet, translates columns, and
# inner-joins to the relevant gear class via source_gear.

## Mobile (ws_dragnot_varpa: OTB / OTM / SDN) ----------------------------------
aux_mobile <-
  read_parquet("data-raw/data-dump/fs_afladagbok/ws_dragnot_varpa.parquet") |>
  wk_translate(dictionary) |>
  inner_join(source_gear |> filter(gear %in% c("OTB", "OTM", "SDN")), by = ".sid")

## Dredge (ws_plogur: DRB) ----------------------------------------------------
aux_dredge <-
  read_parquet("data-raw/data-dump/fs_afladagbok/ws_plogur.parquet") |>
  wk_translate(dictionary) |>
  inner_join(source_gear |> filter(gear == "DRB"), by = ".sid")

## Static (ws_linanethandf: LLS / GNS / GND / LHM) ----------------------------
aux_static <-
  read_parquet("data-raw/data-dump/fs_afladagbok/ws_linanethandf.parquet") |>
  wk_translate(dictionary) |>
  inner_join(source_gear |> filter(gear %in% c("LLS", "GNS", "GND", "LHM")), by = ".sid")

## Traps (ws_gildra: FPO) -----------------------------------------------------
aux_trap <-
  read_parquet("data-raw/data-dump/fs_afladagbok/ws_gildra.parquet") |>
  wk_translate(dictionary) |>
  inner_join(source_gear |> filter(gear == "FPO"), by = ".sid")

## Purse seine (ws_hringn: PS) -------------------------------------------------
aux_seine <-
  read_parquet("data-raw/data-dump/fs_afladagbok/ws_hringn.parquet") |>
  wk_translate(dictionary) |>
  inner_join(source_gear |> filter(gear == "PS"), by = ".sid")

# Fishing sample --------------------------------------------------------------

## Gear dimensions + effort inputs ---------------------------------------------
# One row per station; gear-specific columns NA where not applicable.
# effort_count and effort_unit are derived here alongside gear dimensions —
# they share the same source columns.
dims <-
  bind_rows(
    # OTB / OTM — bottom and midwater trawl
    aux_mobile |>
      filter(gear %in% c("OTB", "OTM")) |>
      mutate(effort_count = coalesce(as.integer(n_units), 1L),
             effort_unit  = "gear-minutes") |>
      select(.sid, n_units, g_mesh, g_width, effort_count, effort_unit),
    # SDN — Danish seine (effort is per setting, not time-based)
    aux_mobile |>
      filter(gear == "SDN") |>
      mutate(effort_count = 1L, effort_unit = "setting") |>
      select(.sid, g_mesh, g_length, effort_count, effort_unit),
    # DRB — dredge / plow
    aux_dredge |>
      mutate(effort_count = coalesce(as.integer(n_units), 1L),
             effort_unit  = "gear-minutes") |>
      select(.sid, n_units, g_mesh, g_width, g_length, g_height, effort_count, effort_unit),
    # LLS — longline (gid 12 Lína, 13 Landbeitt lína, 21 Línutrekt)
    aux_static |>
      filter(gear == "LLS") |>
      mutate(effort_count = n_hooks, effort_unit = "hook-days") |>
      select(.sid, n_units = n_hooks, effort_count, effort_unit),
    # GNS / GND — gillnet
    aux_static |>
      filter(gear %in% c("GNS", "GND")) |>
      mutate(n_units      = n_nets,
             g_length     = n_nets * mean_gillnet_length,
             effort_count = n_units,
             effort_unit  = "net-days") |>
      select(.sid, n_units, g_mesh, g_height, g_length, effort_count, effort_unit),
    # LHM — handline / jig
    aux_static |>
      filter(gear == "LHM") |>
      mutate(effort_count = n_jigs, effort_unit = "jig-hours") |>
      select(.sid, n_units = n_jigs, effort_count, effort_unit),
    # FPO — traps
    aux_trap |>
      mutate(effort_count = n_units, effort_unit = "trap-hours") |>
      select(.sid, n_units, effort_count, effort_unit),
    # PS — purse seine (effort is per setting)
    aux_seine |>
      mutate(effort_count = 1L, effort_unit = "setting") |>
      select(.sid, g_mesh, g_length, g_height, effort_count, effort_unit)
  )

## Duration caps (minutes) per gid ---------------------------------------------
# Mobile tow-time caps and static gear soak-time caps.
gear_caps <- tribble(
  ~gid,  ~cap_m,
     1,  7 * 24 * 60,   # Skötuselsnet  GNS
     2,  7 * 24 * 60,   # Þorskfisknet  GNS
     3,  7 * 24 * 60,   # Grásleppunet  GNS
     4,  7 * 24 * 60,   # Rauðmaganet   GNS
     5,      24 * 60,   # Reknet        GND
     6,      12 * 60,   # Botnvarpa     OTB
     7,      12 * 60,   # Humarvarpa    OTB NEP
     8,      16 * 60,   # Rækjuvarpa    OTB SHR
     9,      30 * 60,   # Flotvarpa     OTM
    11,       4 * 60,   # Dragnót       SDN
    12,      24 * 60,   # Lína          LLS
    13,      24 * 60,   # Landbeitt lína LLS
    14,      24 * 60,   # Handfæri      LHM
    15,      20 * 60,   # Plógur        DRB
    16, 20 * 24 * 60,   # Gildra        FPO
    21,      24 * 60,   # Línutrekt     LLS
    22,  7 * 24 * 60    # Grálúðunet    GNS
)

## Assemble fishing_sample -----------------------------------------------------
fishing_sample <-
  source |>
  select(.tid, .sid, gid, t0, t1, t2, date, n_lost, schema) |>
  left_join(gear_mapping |> select(gid, gid_old = map, gear, target2), by = "gid") |>
  # Duration by gear class:
  #   t1 (milli_timi) is absent for OTB/OTM/DRB, so duration = t2 − t0
  #   for all time-based gears (full operation from warp-entry / gear-set to
  #   hauling end / retrieval end). SDN / PS effort is per setting, no duration.
  mutate(duration_m = case_when(
    gear %in% c("OTB", "OTM", "DRB", "LLS", "GNS", "GND", "LHM", "FPO") ~
      as.numeric(difftime(t2, t0, units = "mins")),
    .default = NA_real_
  )) |>
  # Apply per-gid duration caps
  left_join(gear_caps, by = "gid") |>
  mutate(
    .duration_source = case_when(
      is.na(duration_m)                   ~ "missing",
      !is.na(cap_m) & duration_m > cap_m  ~ "capped",
      .default = "data"
    ),
    duration_m = case_when(
      !is.na(cap_m) & duration_m > cap_m ~ cap_m,
      .default = duration_m
    )
  ) |>
  select(-cap_m) |>
  # Effort: two-component — effort = effort_count × effort_duration
  left_join(dims |> select(.sid, effort_count, effort_unit), by = ".sid") |>
  mutate(
    effort_duration = case_when(
      effort_unit == "gear-minutes" ~ duration_m,
      effort_unit == "hook-days"    ~ duration_m / (60 * 24),
      effort_unit == "net-days"     ~ duration_m / (60 * 24),
      effort_unit == "jig-hours"    ~ duration_m / 60,
      effort_unit == "trap-hours"   ~ duration_m / 60,
      effort_unit == "setting"      ~ 1,
      .default = NA_real_
    ),
    effort = effort_count * effort_duration
  ) |>
  # Gear dimensions
  left_join(dims |> select(-effort_count, -effort_unit), by = ".sid") |>
  select(.tid, .sid, gid, gid_old, gear, target2, t0, t1, t2, date,
         duration_m, .duration_source,
         effort_count, effort_duration, effort_unit, effort,
         n_units, n_lost, g_mesh, g_width, g_length, g_height,
         schema)

# Catch -----------------------------------------------------------------------
catch <-
  read_parquet("data-raw/data-dump/fs_afladagbok/ws_afli.parquet") |>
  wk_translate(dictionary) |>
  select(.sid, sid, catch) |>
  group_by(.sid, sid) |>
  summarise(catch = sum(catch, na.rm = TRUE), .groups = "drop") |>
  inner_join(fishing_sample |> select(.sid), by = ".sid") |>
  arrange(.sid, sid)

# Export -----------------------------------------------------------------------
trip           |> write_parquet("data/fs_afladagbok/trip.parquet")
station        |> write_parquet("data/fs_afladagbok/station.parquet")
fishing_sample |> write_parquet("data/fs_afladagbok/fishing_sample.parquet")
catch          |> write_parquet("data/fs_afladagbok/catch.parquet")


# QC scratch (if FALSE) -------------------------------------------------------
if (FALSE) {

  ## Coordinate format breakdown -----------------------------------------------
  source |> count(coord_fmt)

  ## Duration distributions by gear --------------------------------------------
  fishing_sample |>
    filter(!is.na(duration_m)) |>
    ggplot(aes(duration_m / 60)) +
    geom_histogram() +
    facet_wrap(~gear, scales = "free")

  ## Effort summary ------------------------------------------------------------
  fishing_sample |>
    count(gear, effort_unit, .duration_source)

  ## t-ordering QC (t0 ≤ t1 ≤ t2) ---------------------------------------------
  fishing_sample |>
    filter(!is.na(t0), !is.na(t2)) |>
    summarise(t0_before_t2 = mean(t0 <= t2, na.rm = TRUE),
              t1_before_t2 = mean(t1 <= t2, na.rm = TRUE))

  ## Station coverage over time ------------------------------------------------
  station |>
    mutate(year = year(date)) |>
    count(year) |>
    filter(year >= 2018) |>
    print(n = 20)

}
