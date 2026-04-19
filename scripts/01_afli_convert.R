# Create standard summary tables
# Input:  data-raw/data-dump/afli/*.parquet
# Output: data/afli/trip.parquet, station.parquet, catch.parquet

# Seeding ----------------------------------------------------------------------
library(whack) # pak::pak("einarhjorleifsson/whack")
library(geo)
library(tidyverse)
library(nanoparquet)

SCHEMA <- "afli"
dictionary   <- read_parquet("data/dictionary.parquet") |>
  filter(schema == SCHEMA)
gear_mapping <- read_parquet("data/gear/gear_mapping.parquet") |>
  filter(version == "old")

# base -------------------------------------------------------------------------
#. trip and station table will be built downstream from base
#  station table well be build downstream from base and variables that
#  are in auxillary table (this schema is messy)
base <-
  read_parquet("data-raw/data-dump/afli/stofn.parquet") |>
  wk_translate(dictionary) |>
  # 40 records have NA date — drop them
  filter(!is.na(date)) |>
  mutate(lon1 = -geoconvert.1(x1 * 100),
         lat1 =  geoconvert.1(y1 * 100),
         lon2 = -geoconvert.1(x2 * 100),
         lat2 =  geoconvert.1(y2 * 100)) |>
  select(.sid, vid, gid, date,
         lon1, lat1, lon2, lat2, sq, ssq,
         z1, z2,
         T2, hid2, n_crew,
         everything()) |>
  group_by(vid, T2, hid2) |>
  mutate(.tid = min(.sid)) |>
  group_by(vid, .tid) |>
  mutate(T1   = min(date, na.rm = TRUE),
         hid1 = NA_integer_) |>
  ungroup() |>
  mutate(schema = SCHEMA)

# Trip -------------------------------------------------------------------------
# NOTE: trip table is derived — no raw trip source in this schema.
#       T1 is the minimum fishing date within the trip.
trip <-
  base |>
  select(.tid, vid, T1, hid1, T2, hid2, n_crew, schema) |>
  distinct()

# Station base (columns carried into the auxillary blocks)
# base <- base |>
#  select(.tid, .sid, gid, date, lon1, lat1, lon2, lat2, sq, ssq, z1, z2, schema)

# Effort aux blocks ------------------------------------------------------------
# Each block reads an auxiliary parquet, inner-joins with station to get the
# gid/date it needs for effort computation, then selects only .sid + effort
# columns. All blocks are combined and left-joined onto station, so every station
# in stofn is retained (effort = NA where no aux record exists).

## Mobile (dragnót / varpa / plógur) -------------------------------------------
### Import ---------------------------------------------------------------------
aux_mobile <-
  read_parquet("data-raw/data-dump/afli/toga.parquet") |>
  wk_translate(dictionary) |>
  inner_join(base |>
               select(.sid, gid) |>
               inner_join(gear_mapping |>
                            select(gid, gear) |>
                            filter(gear %in% c("DRB", "OTB", "OTM", "SDN"))) |>
               select(.sid, gid, gear))
### Station info ---------------------------------------------------------------
station_mobile <- aux_mobile |>
  select(.sid,
         hhmm,
         duration_m,
         n_units,            # number of simultaneous gears (tvo_veidarf); used in effort_detail
         bottom_type,
         catch_nperkg,
         gid, gear)
### Sensor info ----------------------------------------------------------------
sensor_mobile <- aux_mobile |>
  select(.sid,
         flag_hoflinum,
         flag_nemi,
         sensor_type,
         temp1_surface,
         temp2_surface,
         temp1_bottom,
         temp2_bottom,
         temp1_headline,
         temp2_headline,
         z1_headline,
         z2_headline,
         gid, gear)
### DRB ------------------------------------------------------------------------
aux_DRB <-
  aux_mobile |>
  filter(gear == "DRB") |>
  # Not sure if mesh size is an appropriate parameter for DRB
  select(.sid, mesh, width = width_plow, height = height_plow, length = length_plow, gid, gear)
### SDN ------------------------------------------------------------------------
aux_SDN <- aux_mobile |>
  filter(gear == "SDN") |>
  select(.sid,
         mesh,
         length_togle,
         length_virle,
         mesh_vod,
         length_headline,
         length_fotreipi,
         gear)
### OTX ------------------------------------------------------------------------
# Work pending
aux_OTX <- aux_mobile |> filter(gear %in% c("OTB", "OTM"))

## Static (lineha: longline, gillnet, handline) --------------------------------
### Import ---------------------------------------------------------------------
aux_static <-
  read_parquet("data-raw/data-dump/afli/lineha.parquet") |>
  rename(.sid = visir) |>
  wk_translate(dictionary) |>
  inner_join(base |>
               select(.sid, gid) |>
               inner_join(gear_mapping |>
                            select(gid, gear) |>
                            filter(gear %in% c("LLS", "GNS", "GND", "LHM"))) |>
               select(.sid, gid, gear))
### Station info ---------------------------------------------------------------
station_static <- aux_static |>
  select(.sid,
         t0,
         t1,
         t2,
         duration_d,
         duration_h,
         gid, gear) |>
  mutate(duration_m =
           case_when(
             !is.na(duration_d) ~ duration_d * 24 * 60,
             !is.na(duration_h) ~ duration_h * 60)) |>
  select(-c(duration_d, duration_h))
### Sensor info ----------------------------------------------------------------
sensor_static <- aux_static |>
  select(.sid, temp1_air, temp1_surface, temp1_bottom, seastate, gid, gear)

### LLS ------------------------------------------------------------------------
aux_LLS <- aux_static |>
  filter(gear == "LLS") |>
  # may need to revisit what is selected
  select(.sid, gid, n_total = n_hooks, n_per_unit = n_hooks_per_set, n_units = n_sets, gear) |>
  filter(!if_all(-.sid, is.na)) |>
  mutate(n_total =
           case_when(!is.na(n_total) ~ n_total,
                     !is.na(n_per_unit) & !is.na(n_units) ~ n_per_unit * n_units,
                     .default = NA))
### GNX ------------------------------------------------------------------------
aux_GNX <- aux_static |>
  filter(gear %in% c("GNS", "GND")) |>
  select(.sid, gid,
         n_units = n_nets,
         mesh,
         height = g_height,
         n_lost,
         mean_gillnet_length,
         gear) |>
  mutate(length = n_units * mean_gillnet_length,
         .before = height) |>
  select(-mean_gillnet_length)

### LHM ------------------------------------------------------------------------
aux_LHM <- aux_static |>
  filter(gear == "LHM") |>
  select(.sid, n_units = n_jigs, n_per_unit = n_hooks, gear)

## traps (gildra) --------------------------------------------------------------
aux_trap <-
  read_parquet("data-raw/data-dump/afli/gildra.parquet") |>
  wk_translate(dictionary) |>
  inner_join(base |>
               select(.sid, gid) |>
               inner_join(gear_mapping |>
                            select(gid, gear) |>
                            filter(gear %in% c("FPO"))) |>
               select(.sid, gid, gear)) |>
  select(.sid, n_units, duration_h, gid, gear) |>
  mutate(duration_m = duration_h * 60) |>
  select(-duration_h)
station_trap <- aux_trap |>
  select(.sid,
         duration_m,
         gid, gear)
aux_FPO <- aux_trap |>
  select(.sid, n_units, gid, gear)

## seine / ring net (hringn) ---------------------------------------------------
aux_seine <-
  read_parquet("data-raw/data-dump/afli/hringn.parquet") |>
  wk_translate(dictionary) |>
  inner_join(base |>
               select(.sid, gid) |>
               inner_join(gear_mapping |>
                            select(gid, gear) |>
                            filter(gear %in% c("PS"))) |>
               select(.sid, gid, gear))
### Station info ---------------------------------------------------------------
station_seine <- aux_seine |>
  select(.sid,
         hhmm,
         comment2 = comment,
         gid, gear)
### Sensor info ----------------------------------------------------------------
sensor_seine <- aux_seine |>
  select(.sid, seastate, gid, gear)
### PS -------------------------------------------------------------------------
aux_PS <- aux_seine |>
  select(.sid, mesh, length = g_length, height = g_height, gid, gear)


# Effort detail ----------------------------------------------------------------
# Two-component effort: effort = effort_count × effort_duration, with
# effort_unit naming the product.
#
# effort_count — the gear-type-specific count:
#   mobile (OTB/OTM/DRB): n_units — number of simultaneous trawls/dredges
#                          (tvo_veidarf); defaults to 1 when unrecorded
#   SDN:                   1 per setting (haul-count not relevant)
#   LLS:                   total hooks (fj_kroka, or onglar × bjod)
#   GNX:                   nets hauled (dregin)
#   LHM:                   jigs (faeri)
#   FPO:                   trap count (gildrur)
#   PS:                    1 per set
#
# effort_duration — capped duration in the natural unit for each gear type;
#   computed below after duration_m is capped in the fishing table.
effort_detail <- bind_rows(
  # Mobile: simultaneous gear count; SDN effort is per-set, not time-based
  aux_mobile |>
    select(.sid, gear, n_units) |>
    mutate(
      effort_count = coalesce(as.integer(n_units), 1L),
      effort_unit  = case_when(
        gear %in% c("OTB", "OTM", "DRB") ~ "gear-minutes",
        gear == "SDN"                     ~ "setting",
        .default = NA_character_
      )
    ) |>
    select(.sid, effort_count, effort_unit),
  aux_LLS |>
    select(.sid, effort_count = n_total) |>
    mutate(effort_unit = "hook-days"),
  aux_GNX |>
    select(.sid, effort_count = n_units) |>
    mutate(effort_unit = "net-days"),
  aux_LHM |>
    select(.sid, effort_count = n_units) |>
    mutate(effort_unit = "jig-hours"),
  aux_FPO |>
    select(.sid, effort_count = n_units) |>
    mutate(effort_unit = "trap-hours"),
  aux_PS  |>
    select(.sid)  |>
    mutate(effort_count = 1L, effort_unit = "setting")
)

# Bind data --------------------------------------------------------------------
## Final auxillary table -------------------------------------------------------
auxillary <-
  bind_rows(aux_LLS,
            aux_GNX,
            aux_LHM,
            aux_DRB,
            aux_SDN,
            aux_PS,
            aux_FPO,
            aux_OTX) |>
  select(.sid, gid, gear,
         starts_with("n_"),
         mesh,
         length, height, width,
         everything())

## Sample detail ---------------------------------------------------------------
fishing_detail <-
  bind_rows(
    station_static,
    station_seine,
    station_trap,
    station_mobile) |>
  select(-c(gid, gear)) |>
  filter(!if_all(-c(.sid), is.na))
## Sample sensor ---------------------------------------------------------------
sensor <-
  bind_rows(sensor_static,
            sensor_seine,
            sensor_mobile) |>
  filter(!if_all(-c(.sid, gid, gear), is.na))


# Final tables -----------------------------------------------------------------
## Trip ------------------------------------------------------------------------
trip <- trip
## Station table ---------------------------------------------------------------
# Not clear if there will be one

## Sample tables ---------------------------------------------------------------
### Sample sensor --------------------------------------------------------------
sensor <- sensor
### Sample fishing -------------------------------------------------------------
fishing <-
  base |>
  select(.tid, .sid, gid, date,
         lon1, lat1, lon2, lat2, sq, ssq,
         z1, z2,
         schema) |>
  left_join(gear_mapping |>
              select(gid, gid_new = map, gear, target2)) |>
  left_join(fishing_detail, by = ".sid")

# QC ---------------------------------------------------------------------------

### SKIP THIS FOR NOW ----------------------------------------------------------
if(FALSE) {
  ## checking gear against landings data -----------------------------------------
  lods_londun <-
    read_parquet("../landings/data-raw/data-dump/kvoti/lods_londun.parquet") |>
    select(.lid = komunr,
           vid = skip_nr,
           D2 = l_dags,
           hid2 = hofn,
           comment = aths,
           stada) |>
    mutate(D2 = as_date(D2))
  lods_londun |>
    filter(vid != 9999) |>
    janitor::get_dupes(vid, hid2, D2) |>
    distinct(vid, hid2, D2)
  lods_londun_distinct <-
    lods_londun |>
    arrange(vid, D2, hid2, .lid) |>
    distinct(vid, D2, hid2, .keep_all = TRUE)
  #  Logbook gear may be wrongly entered, landings gear are a helpful to check
  #  quality.

  test <-
    trip |>
    mutate(D2 = as_date(T2)) |>
    # just a test period - lods start in 1992
    filter(between(D2, ymd("1992-09-01"), ymd("2020-12-31"))) |>
    inner_join(fishing |> select(.tid, gid) |> distinct(.tid, .keep_all = TRUE))
  join <-
    test |>
    left_join(lods_londun_distinct) |>
    mutate(has.lid = !is.na(.lid))
  join |> filter(!has.lid) |> count(hid2) |> arrange(-n)
  join |>
    mutate(D2 = floor_date(D2, "month")) |>
    count(D2, has.lid) |>
    ggplot(aes(D2, n, fill = has.lid)) +
    geom_col(position = "fill")
  join2 <-
    lods_londun_distinct |>
    full_join(test) |>
    mutate(what = case_when(!is.na(.tid) & !is.na(.lid) ~ "tid_lid",
                            is.na(.tid) & !is.na(.lid) ~ "na_lid",
                            !is.na(.tid) &  is.na(.lid) ~ "tid_na",
                            .default = "should not be here"))

  join2 |>
    mutate(D2 = floor_date(D2, "month")) |>
    count(D2, gid, what) |>
    filter(year(D2) > 1993) |>
    ggplot(aes(D2, n, colour = what, fill = what)) +
    geom_point() +
    facet_wrap(~ gid, scales = "free_y")
  # note in the distinct on landings, drop data if different gear (gid_ln) and/or .lid on the same vid and datel pair
  #  probably best to do a qc on the landings data first, ensuring ...
  # match_nearest_date(lods |> select(.lid, vid, datel = D2, gid_ln = gid) |> distinct(vid, datel, .keep_all = TRUE),
  #                   method = "data.table")
  test |>
    count(gid_ln, gid) |>
    group_by(gid_ln) |>
    mutate(p = n / sum(n)) |>
    ungroup() |>
    arrange(gid_ln, desc(p)) |>
    knitr::kable()
  # qc
  # how many missing matches?
  test |> mutate(has.lid = ifelse(!is.na(.lid), "yes", "no")) |> count(has.lid)
  # should ideally only have unique .lid
  test |> select(.tid, .lid) |> janitor::get_dupes()
  test |> select(.tid, .lid) |> distinct() |> janitor::get_dupes()
  test |> select(.tid, .lid) |> distinct() |> janitor::get_dupes(.tid)
  test |> select(.tid, .lid) |> distinct() |> janitor::get_dupes(.lid)
  # test date difference
  test |>
    mutate(dt = difftime(date.ln, datel, units = "days"),
           dt = as.numeric(dt)) |>
    mutate(dt = case_when(dt < -5 ~ -5,
                          dt >  5 ~  5,
                          .default = dt)) |>
    count(dt) |>
    mutate(p = n/sum(n))
  test |>
    mutate(dt = difftime(date.ln, datel, units = "days"),
           dt = as.numeric(dt)) |>
    mutate(flag = case_when(between(dt, -0, 0) ~ "ok",
                            .default = "miss")) |>
    mutate(datel = floor_date(datel, "month")) |>
    count(datel, flag) |>
    ggplot(aes(datel, n, colour = flag, fill = flag)) +
    geom_col(position = "fill")
  # remember the objective here is primary to get the reported landing gear
  test |> count(.lid) |> filter(n > 1) |> arrange(-n)
  # ... hmmmm, so a landing .lid gets mapped to more than one trip
  # even on a date match:
  test |>
    mutate(dt = difftime(date.ln, datel, units = "days"),
           dt = as.numeric(dt)) |>
    filter(dt == 0) |>
    count(.lid) |> filter(n > 1) |> arrange(-n)
  test |> filter(.lid == 9805542)


  library(duckdbfs)
  ais <- open_dataset("/Volumes/fishydata/data/ais/trail")

}


## Cap tables ------------------------------------------------------------------

### Duration -------------------------------------------------------------------
# unit of cap in hours
# may think of upper and lower bounds
# MAY NEED TO THINK ABOUT LOWER CAP, particularly SDN (see graph below)
gear_caps <- tribble(
  ~gear,  ~target2, ~cap_m,
  "OTB",   "DEF",        10 * 60,    # Finfish
  "OTB",   "NEP",         8 * 60,    # Nephrops
  "OTB",   "SHR",        16 * 60,    # Shrim
  "OTM",   "SPF",        24 * 60,    # Pelagics
  "OTM",   "XXX",        24 * 60,    # Pelagics
  "DRB",   "ARI",         4 * 60,    # Kúffiskur - suspiciously low, but in the data
  "DRB",   "ECH",        12 * 60,    # dredge: short, repeated tows.   CHECK
  "DRB",   "SCL",        12 * 60,    # dredge: short, repeated tows
  "SDN",   "DEF",         3 * 60,    # Demarsal seine
  "LLS",   "DEF",        24 * 60,
  "LHM",   "DEF",        24 * 60,    # handline: full day at most
  "GNS",   "DEF",    7 * 24 * 60,    # gillnet: can soak for ~7 days
  "GNS",   "LUM",    7 * 24 * 60,    # gillnet: can soak for ~7 days
  "GNS",   "MON",    7 * 24 * 60,    # gillnet: can soak for ~7 days
  "GNS",   "GHL",    7 * 24 * 60,    # gillnet: can soak for ~7 days
  "GND",   "SPF",             60,    # Drift net - not data in historical dataset
  "FPO",   "GAD",   20 * 24 * 60,    # traps: can soak for ~20 days
  "FPO",   "CRB",   20 * 24 * 60,    # traps: can soak for ~20 days
  "FPO",   "WHL",   20 * 24 * 60     # traps: can soak for ~20 days
)
fishing <- fishing |>
  # talk to Jonas
  mutate(duration_m =
           # discovered by looking a data over time - dominantly hours in
           # earlier period - talk to Jonas
           case_when(gear == "OTB" & target2 == "NEP" & year(date) <= 1999 ~ NA,
                     .default = duration_m)) |>
  left_join(gear_caps) |>
  mutate(.duration_source =
           case_when(is.na(duration_m) ~ "data",
                     duration_m > cap_m ~ "capped",
                     .default = "data"),
         duration_m =
           case_when(is.na(duration_m) ~ NA,
                     duration_m > cap_m ~ cap_m,
                     .default = duration_m)) |>
  select(-cap_m)

## Effort components -----------------------------------------------------------
# effort_duration is derived from the capped duration_m in the natural unit for
# each gear type. For "setting" gears (SDN, PS) effort_duration is fixed at 1.
# effort = effort_count × effort_duration.
fishing <- fishing |>
  left_join(effort_detail, by = ".sid") |>
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
  )

if (FALSE) {
fishing |>
  ggplot(aes(duration_m / 60, fill = target2)) + geom_histogram() +
  facet_wrap(~ gear, scales = "free") + scale_fill_brewer(palette = "Set1")
}

## Derive t1 and t2 from hhmm and duration_m -----------------------------------
fishing <-
  fishing |>
  # one record with t1 value and t2 missing
  # mutate(t1 = case_when(is.na(t2) & !is.na(t1) ~ NA,
  #                       .default = t1)) |>
  mutate(.t1_source = ifelse(!is.na(t1), "data", "derived"),
         .t2_source = ifelse(!is.na(t2), "data", "derived")) |>
  mutate(duration_m = case_when(duration_m <= 0 ~ NA,      # should have a lower cap
                                .default = duration_m)) |>
  mutate(hhmm = case_when(hhmm >= 100 ~ hhmm,
                          .default = NA),
         hh = as.integer(hhmm %/% 100),
         hh = ifelse(hh < 24, hh, NA),
         mm = as.integer(hhmm %% 100),
         mm = ifelse(mm < 60 & !is.na(hh), mm, NA),
         t0 =
           case_when(!is.na(t0) ~ t0,
                     .default = NA),
         t1 =
           case_when(!is.na(t1) ~ t1,
                     !is.na(hh) & !is.na(mm) ~ as_datetime(date) + hours(hh) + minutes(mm),
                     .default = NA),
         t2 =
           case_when(!is.na(t2) ~ t2,
                     # !is.na(t1) ~ NA,
                     !is.na(t1) & !is.na(duration_m) ~ t1 + minutes(as.integer(duration_m)),
                     .default = NA))


if (FALSE) {
fishing |> count(.t1_source, .t2_source)
fishing |>
  ggplot(aes(duration_m / 60, fill = .t1_source)) + geom_histogram() +
  facet_wrap(~ gear, scales = "free") + scale_fill_brewer(palette = "Set1")
fishing |>
  mutate(.ebook = ifelse(.sid < 0, "yes", "no")) |>
  mutate(year = year(date)) |>
  count(year, .ebook, gear) |>
  filter(year >= 1973) |>
  ggplot(aes(year, n, colour = .ebook)) +
  geom_point() + facet_wrap(~ gear, scales = "free_y")
}

### QC: check for overlapping trips --------------------------------------------
## Need to make a decision what to do here - note the chronology as well
if(FALSE) {
  fishing |>
    # trips need to be distincts - check upstream why not
    inner_join(trip |> select(.tid, vid) |> distinct()) |>
    arrange(vid, date, t1) |>
    osfd::fd_flag_time_overlaps(t1, t2, by = vid) |>
    mutate(ebook = ifelse(.sid < 0, "yes", "no")) |>
    count(.t1_source, .checks) |>
    filter_out(.checks %in% c("end time missing", "start time missing")) |>
    group_by(.t1_source) |>
    mutate(p = n / sum(n)) |>
    ungroup()
  tmp <-
    test |>
    inner_join(trip |> select(.tid, vid) |> distinct()) |>
    arrange(vid, date, t1) |>
    osfd::fd_flag_time_overlaps(t1, t2, by = vid) |>
    mutate(ebook = ifelse(.sid < 0, "yes", "no"))
  tmp |>
    filter(.t1_source == "derived") |>
    ggplot(aes(duration_m / 60, fill = .checks)) + geom_histogram() +
    facet_wrap(~ gear, scales = "free") + scale_fill_brewer(palette = "Set1")
  tmp |>
    #filter(.t1_source != "derived") |>
    mutate(year = year(date)) |>
    count(year, .checks, gear) |>
    filter(year >= 1973) |>
    ggplot(aes(year, n, fill = .checks)) +
    geom_col() +
    facet_wrap(~ gear, scales = "free_y") + scale_fill_brewer(palette = "Set1")
}

### Make t1 and t2 where overlap checks not "ok" as NA -------------------------
fishing <-
  fishing |>
  inner_join(trip |> select(.tid, vid) |> distinct()) |>
  arrange(vid, date, t1) |>
  osfd::fd_flag_time_overlaps(t1, t2, by = vid) |>
  mutate(t1 = case_when(.checks != "ok" ~ NA,
                        .default = t1),
         t2 = case_when(.checks != "ok" ~ NA,
                        .default = t2))

## Next thing ------------------------------------------------------------------



# Catch ------------------------------------------------------------------------
catch <-
  read_parquet("data-raw/data-dump/afli/afli.parquet") |>
  wk_translate(dictionary, "messy", "clean") |>
  select(.sid, sid, catch) |>
  group_by(.sid, sid) |>
  summarise(catch = sum(catch, na.rm = TRUE), .groups = "drop") |>
  # retain only catch records linked to a known station
  inner_join(fishing |> select(.sid, schema), by = ".sid") |>
  arrange(.sid, sid)

# Add "new" gear codes ---------------------------------------------------------
station <- fishing |>
  rename(gid_old = gid) |>
  left_join(gear_mapping |>
              filter(version == "old") |>
              select(gid_old = gid, gid = map),
            by = "gid_old") |>
  select(.tid, .sid, gid, gid_old, everything())

# Export -----------------------------------------------------------------------
trip    |> write_parquet("data/afli/trip.parquet")
station |> write_parquet("data/afli/station.parquet")
sensor  |> write_parquet("data/afli/sensor.parquet")
catch   |> write_parquet("data/afli/catch.parquet")
