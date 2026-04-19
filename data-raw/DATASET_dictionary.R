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
  ~clean,                ~messy,          ~level,
  ".sid",                "visir",         "station",   # station primary key
  "vid",                 "skipnr",        "trip",      # vessel — trip attr, denorm. onto station
  "gid",                 "veidarf",       "station",   # gear type defines station type
  "n_crew",              "ahofn",         "trip",      # crew count — trip-level attribute
  "date",                "vedags",        "station",
  "x1",                  "lengd",         "station",
  "y1",                  "breidd",        "station",
  "x2",                  "lengd_lok",     "station",
  "y2",                  "breidd_lok",    "station",
  "sq",                  "reitur",        "station",
  "ssq",                 "smareitur",     "station",
  "z1",                  "dypi",          "station",
  "z2",                  "dypi_lok",      "station",
  "winddirection",       "vindatt",       "station",   # environmental — recorded at station
  "beaufort",            "vindstig",      "station",
  "m_sec",               "m_sek",         "station",
  "T2",                  "ldags",         "trip",      # anticipated landing date
  "hid2",                "lhofn",         "trip") |>   # landing harbour
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
  ~clean,                   ~messy,                ~level,
  ### time and duration --------------------------------------------------------
  "hhmm",                   "ibotni",              "sample",   # time of bottom contact
  "duration_m",             "togtimi",             "sample",   # tow duration (minutes)
  ### sensors ------------------------------------------------------------------
  "sensor_type",            "gerd_maelis",         "sample",
  "flag_nemi",              "nemi",                "sample",   # char code, not 0, 1, NA
  "flag_hoflinum",          "hoflinum",            "sample",   # char code, not 0, 1, NA
  "temp1_surface",          "uppsj_hiti",          "sample",
  "temp2_surface",          "uppsj_hiti_lok",      "sample",
  "temp1_bottom",           "botnhiti",            "sample",
  "temp2_bottom",           "botnhiti_lok",        "sample",
  "temp1_headline",         "hoflinu_hiti",        "sample",
  "temp2_headline",         "hoflinu_hiti_lok",    "sample",
  "z1_headline",            "hoflinu_dypi",        "sample",
  "z2_headline",            "hoflinu_dypi_lok",    "sample",
  ### orphans ------------------------------------------------------------------
  "bottom_type",            "botn_gerd",           "station",  # habitat — location attribute
  "catch_nperkg",           "fjikilo",             "sample",
  ### gear specifications ------------------------------------------------------
  "n_units",                "tvo_veidarf",         "sample",
  "mesh",                   "moskvi",              "sample",
  "mesh_type",              "moskvi_gerd",         "sample",
  "mesh_min",               "moskvi_minnsti",      "sample",
  "mesh_max",               "moskvi_staersti",     "sample",
  "mesh_vod",               "moskvi_vod",          "sample",
  "mesh_size",              "moskvi_staerd",       "sample",
  "mesh_size2",             "moskvi2_staerd",      "sample",
  "mesh_fj_ummali",         "moskvi_fj_ummali",    "sample",
  "mesh_fj_ummali2",        "moskvi2_fj_ummali",   "sample",
  "doors_kg",               "hlerar",              "sample",
  "flag_legggluggi",        "legggluggi",          "sample",   # 0, 1, NA
  "flag_leggpoki",          "leggpoki",            "sample",   # 0, 1, NA
  "length_headline",        "hoflina",             "sample",
  "length_grandarar",       "grandarar",           "sample",
  "length_grandarar_top",   "efri_grandarar",      "sample",
  "length_fotreipi",        "fotreipi",            "sample",
  "length_togle",           "togle",               "sample",
  "length_virle",           "virle",               "sample",
  "length_plow",            "pl_lengd",            "sample",
  "width_plow",             "pl_breidd",           "sample",
  "height_plow",            "pl_haed",             "sample",
  ### codend -------------------------------------------------------------------
  "g_codend_length",        "lengd_poka",          "sample",   # codend length
  "g_codend_type",          "gerd_poka",           "sample",   # codend type
  ### mesh / net geometry ------------------------------------------------------
  "mesh_belly",             "moskvi_belg_staerd",  "sample",   # belly mesh size
  "mesh_circ_count",        "staerd",              "sample",   # mesh count around circumference
  "g_circumference",        "ummal",               "sample",   # wing-belly junction circumference (m)
  ### trawl hardware -----------------------------------------------------------
  "g_door_area",            "fermetrar",           "sample",   # otter-board area (sq.m)
  "g_grid_type",            "skge_nr",             "sample") |> # sorting-grid type (small fish/shrimp)
  # NOTE: toga columns not mapped: nafn_vorpu (trawl name, free text),
  #       gerd (shrimp-trawl sub-type ref, deprecated), fj_byrda (unknown),
  #       sk_rimlabil (unknown)
  mutate(schema = "afli",
         table = "toga")
## afli.lineha -----------------------------------------------------------------
afli_lineha <- tibble::tribble(
  ~clean,                ~messy,              ~level,
  ### time — sample events (grammar t0–t2 for static gear) --------------------
  "t0",                  "logn_hefst",        "sample",  # gear deployment starts
  "t1",                  "drattur_hefst",     "sample",  # retrieval starts
  "t2",                  "drattur_lykur",     "sample",  # retrieval ends
  ### duration (effort) --------------------------------------------------------
  "duration_d",          "naetur",            "sample",  # soak time (days)
  "duration_h",          "klst",              "sample",  # soak time (hours)
  ### orphans ------------------------------------------------------------------
  "seastate",            "sjolag",            "station", # environmental — location attribute
  ### sensors ------------------------------------------------------------------
  "temp1_air",           "lofthiti",          "sample",
  "temp1_surface",       "uppsjavarhiti",     "sample",
  ### gear specifications -------------------------------------------------------
  "mean_gillnet_length", "medal_lengd_neta",  "sample",
  "bait",                "beita",             "sample",
  ### longline -----------------------------------------------------------------
  "n_hooks",             "fj_kroka",          "sample",  # total hook count
  "n_hooks_per_set",     "onglar",            "sample",  # hooks per set (bait piece)
  "n_sets",              "bjod",              "sample",  # number of sets (bait runs)
  ### gillnet ------------------------------------------------------------------
  "n_nets",              "dregin",            "sample",  # nets hauled
  "g_height",            "haed",              "sample",  # mesh height (count of meshes)
  # NOTE: g_height reused in hringn for net depth (m) and in fs_afladagbok for
  #       both; unit differs by gear type (mesh count here, metres in hringn)
  "n_lost",              "fj_tap_neta",       "sample",  # lost / damaged nets
  ### handline -----------------------------------------------------------------
  "n_jigs",              "faeri",             "sample") |> # number of jigs / lures
  mutate(schema = "afli",
         table = "lineha")

## afli.gildra ----------------------------------------------------------------
afli_gildra <- tibble::tribble(
  ~clean,      ~messy,      ~level,
  "n_units",   "gildrur",   "sample"  # trap count; klst -> duration_h already in afli_lineha
) |>
  mutate(schema = "afli",
         table = "gildra")

## afli.hringn ----------------------------------------------------------------
afli_hringn <- tibble::tribble(
  ~clean,      ~messy,        ~level,
  "g_length",  "l_notar",     "sample",  # net length (m)
  "g_height",  "d_notar",     "sample",  # net depth (m) — NOTE: same clean name as mesh height
                                          # in lineha but different unit (m vs mesh count)
  "hhmm",      "klukkan",     "sample",  # time of set
  "comment",   "athskipst",   "sample"   # skipper's remark
  # sjolag -> seastate, moskvi -> mesh, visir -> .sid already covered schema-wide
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
  "t0",    "fishing_start",     "adb",    # gear deployment (static)
  "t1",    "tow_start",         "adb",    # hauling starts
  "t2",    "fishing_end",       "adb",    # hauling ends / deployment ends
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
  "t0",     "upphaf_timi",           # gear deployment (static)
  "t1",     "milli_timi",            # hauling starts
  "t2",     "lok_timi",              # hauling ends / deployment ends
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
  "n_hooks", "fj_kroka",
  "n_nets",  "fj_dreginna_neta",
  "n_jigs",  "fj_faera",
  "g_mesh",  "moskvi_staerd",
  "g_height", "moskvi_haed",
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
