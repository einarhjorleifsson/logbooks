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
afli <- tibble::tribble(
  ~clean,                ~messy,               ~schema,
  ".sid",                "visir",             "afli",
  "vid",                 "skipnr",            "afli",
  "gid",                 "veidarf",           "afli",
  "n_crew",              "ahofn",             "afli",
  "date",                "vedags",            "afli",
  "x1",                  "lengd",             "afli",
  "y1",                  "breidd",            "afli",
  "x2",                  "lengd_lok",         "afli",
  "y2",                  "breidd_lok",        "afli",
  "sq",                  "reitur",            "afli",
  "ssq",                 "smareitur",         "afli",
  "z1",                  "dypi",              "afli",
  "z2",                  "dypi_lok",          "afli",
  "winddirection",       "vindatt",           "afli",
  "beaufort",            "vindstig",          "afli",
  "m_sec",               "m_sek",             "afli",
  "distance",            "toglengd",          "afli",
  "T2",                  "ldags",             "afli",   # is actually anticipated date
  "hid2",                "lhofn",             "afli",
  "sid",                 "tegund",            "afli",
  "catch",               "afli",              "afli",
  "towtime",             "togtimi",           "afli",
  "on.bottom",           "ibotni",            "afli",
  "mesh",                "moskvi",            "afli",
  "mesh_min",            "moskvi_minnsti",    "afli",
  "doors",               "hlerar",            "afli",
  "headline",            "hoflina",           "afli",
  "sweeps",              "grandarar",         "afli",
  "plow_width",          "pl_breidd",         "afli",
  "tempb1",              "botnhiti",          "afli",
  "tempb2",              "botnhiti_lok",      "afli",
  "temps1",              "uppsj_hiti",        "afli",    # toga.parquet
  "temps2",              "uppsj_hiti_lok",    "afli",
  "height",              "haed",              "afli",
  "mean_gillnet_length", "medal_lengd_neta",  "afli",
  "bait",                "beita",             "afli",
  "temps1",              "uppsjavarhiti",     "afli",    # lineha.parquet
  "t0",                  "logn_hefst",        "afli",
  "t1",                  "drattur_hefst",     "afli",
  "t2",                  "drattur_lykur",     "afli",
  "n_units",             "gildrur",           "afli",
  "hours",               "klst",              "afli",
  "vessel",              "skip_nafn",         "afli",
  "cs",                  "kallmerki",         "afli",
  "uid",                 "umdaemisnumer",     "afli",
)

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
)

# fs_afladagbok ----------------------------------------------------------------
fs_afladagbok <- tibble::tribble(
  ~clean,     ~messy,                      ~schema,
  # ws_veidiferd.parquet (trips)
  # ".tid",   "id",                       "fs_afladagbok",  # Ambiguous across tables
  ".tid",   "veidiferd_id",             "fs_afladagbok",
  "vid",    "skipnr",                   "fs_afladagbok",
  "T1",     "upphafstimi",              "fs_afladagbok",
  "hid1",   "upphafshofn",              "fs_afladagbok",
  "T2",     "londunardagur",            "fs_afladagbok",
  "hid2",   "londunarhofn",             "fs_afladagbok",
  "source", "uppruni",                  "fs_afladagbok",
  # ws_veidi.parquet (stations)
  #".sid",   "veidi_id",                 "fs_afladagbok",
  "gid",    "veidarfaeri_id",           "fs_afladagbok",
  "t0",     "upphaf_timi",              "fs_afladagbok",   # gear deployment (static)
  "t1",     "milli_timi",               "fs_afladagbok",   # hauling starts
  "t2",     "lok_timi",                 "fs_afladagbok",   # hauling ends / deployment ends
  "x1",     "upphaf_lengd",             "fs_afladagbok",
  "y1",     "upphaf_breidd",            "fs_afladagbok",
  "x2",     "lok_lengd",                "fs_afladagbok",
  "y2",     "lok_breidd",               "fs_afladagbok",
  "z1",     "upphaf_dypi",              "fs_afladagbok",
  "z2",     "lok_dypi",                 "fs_afladagbok",
  "n_lost",  "fj_tapadra_veidarfaera",   "fs_afladagbok",
  "sweeps",  "grandarar_lengd",          "fs_afladagbok",
  # shared foreign key across gear sub-tables (ws_dragnot_varpa, ws_linanethandf, ws_gildra, ws_hringn, ws_plogur)
  ".sid",    "veidi_id",                 "fs_afladagbok",
  # ws_linanethandf columns
  "n_hooks", "fj_kroka",                 "fs_afladagbok",
  "n_nets",  "fj_dreginna_neta",         "fs_afladagbok",
  "n_jigs",  "fj_faera",                 "fs_afladagbok",
  # ws_gildra columns
  "n_units", "fj_gildra",                "fs_afladagbok",
  # ws_afli columns
  "sid",     "tegund_id",                "fs_afladagbok",
  "catch",   "afli",                     "fs_afladagbok"
)

# ------------------------------------------------------------------------------
dictionary <- bind_rows(afli, adb, fs_afladagbok)
dictionary |> write_parquet("data/dictionary.parquet")
