# Column name translation dictionary — hand-maintained
# Input:  (none)
# Output: data/dictionary.parquet
#
# Usage in convert scripts:
#   dictionary |> dplyr::filter(schema == "afli") |> wk_translate(d, from = "name", to = "new")
#
# Add new rows to the appropriate schema block as new tables are processed.
# Re-source this script to rebuild data/dictionary.parquet.
#
#' Structure: Three named blocks (afli, adb, fs_afladagbok) that bind_rows()
#'   into a single dictionary. To add a new mapping, find the right block and
#'   add a row — then re-source the file.
#'
#' In convert scripts, change the wk_translate calls to filter first:
#'   dictionary |> filter(schema == "adb") |> wk_translate(d, "name", "new")

library(dplyr)
library(nanoparquet)

# afli -------------------------------------------------------------------------
## afli.stofn ----------------------------------------------------------------
afli_stofn <- tibble::tribble(
  ~clean,                ~messy,          ~level,     ~sample_type,
  ".sid",                "visir",         "station",   NA,        # station primary key
  "vid",                 "skipnr",        "trip",      NA,        # vessel — trip attr, denorm. onto station
  "gid",                 "veidarf",       "sample",    "fishing", # gear type defines station type
  "n_crew",              "ahofn",         "trip",      NA,        # crew count — trip-level attribute
  "date",                "vedags",        "station",   NA,
  "month",               "veman",         "station",   NA,
  "x1",                  "lengd",         "station",   NA,
  "y1",                  "breidd",        "station",   NA,
  "x2",                  "lengd_lok",     "station",   NA,
  "y2",                  "breidd_lok",    "station",   NA,
  "sq",                  "reitur",        "station",   NA,
  "ssq",                 "smareitur",     "station",   NA,
  "orreitur",            "orrreitur",     "station",   NA,
  "skiki",               "skiki",         "station",   NA,
  "fj_reitur",           "fj_reitur",     "station",   NA,
  "fj_skiki",            "fj_skiki",      "station",   NA,
  "z1",                  "dypi",          "sample",    "sensor",
  "z2",                  "dypi_lok",      "sample",    "sensor",
  "winddirection",       "vindatt",       "sample",    "sensor",   # sensorironmental — recorded at station
  "beaufort",            "vindstig",      "sample",    "sensor",
  "m_sec",               "m_sek",         "sample",    "sensor",
  "T2",                  "ldags",         "trip",      NA,      # anticipated landing date
  "hid2",                "lhofn",         "trip",      NA,
  "comment",             "aths_texti",    "station",   NA,
  "catch_est",           "aaetladur_afli", "sample",   "fishing") |>   # landing harbour
  mutate(schema = "afli",
         table = "stofn")
## afli.afli -------------------------------------------------------------------
afli_afli <- tibble::tribble(
  ~clean,                ~messy,    ~level,
  "sid",                 "tegund",  "catch",
  "catch",               "afli",    "catch") |>
  mutate(schema = "afli",
         table = "afli")
## afli.toga -------------------------------------------------------------------
afli_toga <- tibble::tribble(
  ~clean,                   ~messy,                ~level,   ~sample_type,
  ### time and duration --------------------------------------------------------
  "hhmm",                   "ibotni",              "sample",   "fishing", # time of bottom contact
  "duration_m",             "togtimi",             "sample",   "fishing",   # tow duration (minutes)
  ### sensors ------------------------------------------------------------------
  "sensor_type",            "gerd_maelis",         "sample",   "sensor",
  "flag_nemi",              "nemi",                "sample",   "sensor",   # char code, not 0, 1, NA
  "flag_hoflinum",          "hoflinum",            "sample",   "sensor",   # char code, not 0, 1, NA
  "temp1_surface",          "uppsj_hiti",          "sample",   "sensor",
  "temp2_surface",          "uppsj_hiti_lok",      "sample",   "sensor",
  "temp1_bottom",           "botnhiti",            "sample",   "sensor",
  "temp2_bottom",           "botnhiti_lok",        "sample",   "sensor",
  "temp1_headline",         "hoflinu_hiti",        "sample",   "sensor",
  "temp2_headline",         "hoflinu_hiti_lok",    "sample",   "sensor",
  "z1_headline",            "hoflinu_dypi",        "sample",   "sensor",
  "z2_headline",            "hoflinu_dypi_lok",    "sample",   "sensor",
  ### orphans ------------------------------------------------------------------
  "bottom_type",            "botn_gerd",           "sample",   "sensor",          # environmental state
  "catch_nperkg",           "fjikilo",             "sample",   "NA",         # this an orphan
  ### gear specifications ------------------------------------------------------
  "n_units",                "tvo_veidarf",         "sample",   "fishing",
  "mesh",                   "moskvi",              "sample",   "fishing",
  "mesh_type",              "moskvi_gerd",         "sample",   "fishing",
  "mesh_min",               "moskvi_minnsti",      "sample",   "fishing",
  "mesh_max",               "moskvi_staersti",     "sample",   "fishing",
  "mesh_vod",               "moskvi_vod",          "sample",   "fishing",
  "mesh_size",              "moskvi_staerd",       "sample",   "fishing",
  "mesh_size2",             "moskvi2_staerd",      "sample",   "fishing",
  "mesh_fj_ummali",         "moskvi_fj_ummali",    "sample",   "fishing",
  "mesh_fj_ummali2",        "moskvi2_fj_ummali",   "sample",   "fishing",
  "doors_kg",               "hlerar",              "sample",   "fishing",
  "flag_legggluggi",        "legggluggi",          "sample",   "fishing",   # 0, 1, NA
  "flag_leggpoki",          "leggpoki",            "sample",   "fishing",   # 0, 1, NA
  "length_headline",        "hoflina",             "sample",   "fishing",
  "length_grandarar",       "grandarar",           "sample",   "fishing",
  "length_grandarar_top",   "efri_grandarar",      "sample",   "fishing",
  "length_fotreipi",        "fotreipi",            "sample",   "fishing",
  "length_togle",           "togle",               "sample",   "fishing",
  "length_virle",           "virle",               "sample",   "fishing",
  "length_plow",            "pl_lengd",            "sample",   "fishing",
  "width_plow",             "pl_breidd",           "sample",   "fishing",
  "height_plow",            "pl_haed",             "sample",   "fishing",
  "nafn_vorpu",             "nafn_vorpu",          "sample",   "fishing",
  "fj_byrda",               "fj_byrda",            "sample",   "fishing",
  "sk_rimlabil",            "sk_rimlabil",         "sample",   "fishing",
  "gerd",                   "gerd",                "sample",   "fishing",
  ### codend -------------------------------------------------------------------
  "g_codend_length",        "lengd_poka",          "sample",   "fishing",   # codend length
  "g_codend_type",          "gerd_poka",           "sample",   "fishing",   # codend type
  ### mesh / net geometry ------------------------------------------------------
  "mesh_belly",             "moskvi_belg_staerd",  "sample",   "fishing",   # belly mesh size
  "mesh_circ_count",        "staerd",              "sample",   "fishing",   # mesh count around circumference
  "g_circumference",        "ummal",               "sample",   "fishing",   # wing-belly junction circumference (m)
  ### trawl hardware -----------------------------------------------------------
  "g_door_area",            "fermetrar",           "sample",   "fishing",   # otter-board area (sq.m)
  "g_grid_type",            "skge_nr",             "sample",   "fishing") |> # sorting-grid type (small fish/shrimp)
  # NOTE: toga columns not mapped: nafn_vorpu (trawl name, free text),
  #       gerd (shrimp-trawl sub-type ref, deprecated), fj_byrda (unknown),
  #       sk_rimlabil (unknown)
  mutate(schema = "afli",
         table = "toga")
## afli.lineha -----------------------------------------------------------------
afli_lineha <- tibble::tribble(
  ~clean,                ~messy,              ~level,   ~sample_type,
  ### time — sample events -------------------------------------------------------
  # Grammar: four events per fishing operation, t1–t4 in order of occurrence:
  #   t1 = gear deployment starts (Event 1) — first buoy / hook / warp in water
  #   t2 = gear deployment ends  (Event 2) — UNRECORDED; added as NA in convert script
  #   t3 = gear retrieval starts (Event 3) — first buoy / hook hauled
  #   t4 = gear retrieval ends   (Event 4) — last buoy / hook on board
  # For mobile gear t4 is also unrecorded (NA). Effort = t3 − t2; approximated
  # as t3 − t1 when t2 is absent (i.e. always currently).
  "t1",                  "logn_hefst",        "sample",  "fishing", # Event 1: gear deployment starts
  "t3",                  "drattur_hefst",     "sample",  "fishing", # Event 3: gear retrieval starts (t2 unrecorded)
  "t4",                  "drattur_lykur",     "sample",  "fishing", # Event 4: gear retrieval ends
  ### duration (effort) --------------------------------------------------------
  "duration_d",          "naetur",            "sample",  "fishing",  # soak time (days)
  "duration_h",          "klst",              "sample",  "fishing",  # soak time (hours)
  ### orphans ------------------------------------------------------------------
  "seastate",            "sjolag",            "sample",  "sensor",    # environmental — location attribute
  ### sensors ------------------------------------------------------------------
  "temp1_air",           "lofthiti",          "sample",  "sensor",
  "temp1_surface",       "uppsjavarhiti",     "sample",  "sensor",
  ### gear specifications -------------------------------------------------------
  "mean_gillnet_length", "medal_lengd_neta",  "sample",  "fishing",
  "bait",                "beita",             "sample",  "fishing",
  ### longline -----------------------------------------------------------------
  "n_hooks",             "fj_kroka",          "sample",  "fishing",  # total hook count
  "n_hooks_per_set",     "onglar",            "sample",  "fishing",  # hooks per set (bait piece)
  "n_sets",              "bjod",              "sample",  "fishing",  # number of sets (bait runs)
  ### gillnet ------------------------------------------------------------------
  "n_nets",              "dregin",            "sample",  "fishing",  # nets hauled
  "g_height",            "haed",              "sample",  "fishing",  # mesh height (count of meshes)
  # NOTE: g_height reused in hringn for net depth (m) and in fs_afladagbok for
  #       both; unit differs by gear type (mesh count here, metres in hringn)
  "n_lost",              "fj_tap_neta",       "sample",  "fishing",  # lost / damaged nets
  ### handline -----------------------------------------------------------------
  "n_jigs",              "faeri",             "sample",   "fishing") |> # number of jigs / lures
  mutate(schema = "afli",
         table = "lineha")

## afli.gildra ----------------------------------------------------------------
afli_gildra <- tibble::tribble(
  ~clean,      ~messy,      ~level,   ~sample_type,
  "n_units",   "gildrur",   "sample", "fishing"  # trap count; klst -> duration_h already in afli_lineha
) |>
  mutate(schema = "afli",
         table = "gildra")

## afli.hringn ----------------------------------------------------------------
afli_hringn <- tibble::tribble(
  ~clean,      ~messy,        ~level,   ~sample_type,
  "g_length",  "l_notar",     "sample", "fishing",  # net length (m)
  "g_height",  "d_notar",     "sample", "fishing",  # net depth (m) — NOTE: same clean name as mesh height
                                          # in lineha but different unit (m vs mesh count)
  "kast_nr",   "kast_nr",     "sample",  "fishing",
  "hhmm",      "klukkan",     "sample",  "fishing", # time of set
  #"seastate",  "sjolag",      "sample",   "sensor",
  "comment",   "athskipst",   "sample",    "fishing"  # skipper's remark
) |>
  mutate(schema = "afli",
         table = "hringn")

# adb --------------------------------------------------------------------------
adb <- tibble::tribble(
  ~clean,    ~messy,               ~schema,
  # trip_v.parquet
  ".tid",  "trip_id",           "adb",
  "vid",   "vessel_no",         "adb",
  "T1",    "departure",         "adb",
  "hid1",  "departure_port_no", "adb",
  "T2",    "landing",           "adb",
  "hid2",  "landing_port_no",   "adb",
  # station_v.parquet
  ".sid",  "station_id",        "adb",
  "gid",   "gear_no",           "adb",
  # time — sample events (grammar t1–t4; see afli_lineha comment for full definition)
  "t1",    "fishing_start",     "adb",    # Event 1: gear deployment starts
  "t3",    "tow_start",         "adb",    # Event 3: gear retrieval starts (t2 unrecorded)
  "t4",    "fishing_end",       "adb",    # Event 4: gear retrieval ends
  "lon1",  "longitude",         "adb",    # already decimal degrees — wrong convertion in many cases
  "lat1",  "latitude",          "adb",    # already decimal degrees — wrong convertion in many cases
  "lon2",  "longitude_end",     "adb",    # already decimal degrees — wrong convertion in many cases
  "lat2",  "latitude_end",      "adb",    # already decimal degrees — wrong convertion in many cases
  "z1",    "depth",             "adb",
  "z2",    "depth_end",         "adb",
  "sweeps", "bridle_length",    "adb"
) |>
  mutate(level = NA_character_)

# fs_afladagbok ----------------------------------------------------------------
fs_afladagbok <- tibble::tribble(
  ~clean,     ~messy,
  # ws_veidiferd.parquet (trips)
  # ".tid",   "id",                                        # Ambiguous across tables
  ".tid",   "veidiferd_id",
  "vid",    "skipnr",
  "T1",     "upphafstimi",
  "hid1",   "upphafshofn",
  "T2",     "londunardagur",
  "hid2",   "londunarhofn",
  "source", "uppruni",
  # ws_veidi.parquet (stations)
  #".sid",   "veidi_id",
  "gid",    "veidarfaeri_id",
  # time — sample events (grammar t1–t4; see afli_lineha comment for full definition)
  "t1",     "upphaf_timi",           # Event 1: gear deployment starts
  "t3",     "milli_timi",            # Event 3: gear retrieval starts (absent for OTB/OTM/DRB → NA)
  "t4",     "lok_timi",              # Event 4: gear retrieval ends
  "x1",     "upphaf_lengd",
  "y1",     "upphaf_breidd",
  "x2",     "lok_lengd",
  "y2",     "lok_breidd",
  "z1",     "upphaf_dypi",
  "z2",     "lok_dypi",
  "n_lost",  "fj_tapadra_veidarfaera",

  # shared foreign key across gear sub-tables (ws_dragnot_varpa, ws_linanethandf, ws_gildra, ws_hringn, ws_plogur)
  ".sid",    "veidi_id",
  # ws_dragnot_varpa
  "n_units", "tvo_veidarf",
  "g_mesh",  "moskvi_staerd_poki",
  "g_width", "grandarar_lengd",           # Grandarar used as proxy for 'hlerabil'
  # ws_plogur
  "g_mesh",  "moskvi",
  "g_width", "breidd",
  "g_length", "lengd",
  "g_height", "haed",
  "g_length", "toglengd",                 # this is for danish sein, length of of the "tóg"
  # ws_linanethandf columns
  "n_hooks",            "fj_kroka",
  "n_nets",             "fj_dreginna_neta",
  "n_jigs",             "fj_faera",
  "g_mesh",             "moskvi_staerd",
  "g_height",           "moskvi_haed",
  "mean_gillnet_length", "medal_lengd_neta",
  # ws_hrignot
  "g_length",  "lengd_notu",
  "g_height",   "dypt_notu",
  # ws_gildra columns
  "n_units", "fj_gildra",
  # ws_afli columns
  "sid",     "tegund_id",
  "catch",   "afli",
) |>
  mutate(schema = "fs_afladagbok",
         level = NA_character_)
# ------------------------------------------------------------------------------
dictionary <- bind_rows(afli_stofn,
                        afli_toga,
                        afli_lineha,
                        afli_afli,
                        afli_gildra,
                        afli_hringn,
                        adb,
                        fs_afladagbok)
dictionary |> write_parquet("data/dictionary.parquet")
