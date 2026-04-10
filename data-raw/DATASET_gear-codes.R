# Gear vocabulary — hand-maintained
# Input:  (none)
# Output: data/gear/gear_mapping.parquet

# Notes on gear codes (gid) by data-source (schema):
#  schema afli:         uses historical gear code values (version == "old" below)
#  schema fs_afladagbok: uses the new code values       (version == "new" below)
#  schema adb:          a messy in-house attempt to convert new codes to historical;
#                       no attempt is made here to fix that mess

# task: to be filled in
# On gears
#  The primary gear code used in the ais/vms analysis is that reported in the
#  gafl/landings database.
#
# TODO:
#  * Add criterion for maximum duration
#  * Add criterion for maximum distance

library(tidyverse)


asfis <- read_parquet("data-raw/data-dump/gear/asfis.parquet")
# create table -----------------------------------------------------------------

read_parquet("data-raw/data-dump/afli/stofn.parquet") |>
  pull(veidarf) |> unique() |> sort()


# The `map` column is a one-way canonical lookup between versions:
#   For "new" rows: map = corresponding old gid
#   For "old" rows: map = corresponding new gid
# The mapping is many-to-one in both directions — the old schema used more
# granular sub-types that collapse to fewer new codes (e.g. three mesh sizes of
# seine net → one "Dragnót"; five midwater trawl types → one "Flotvarpa").
# Consequence: round-trips old → new → old and new → old → new are lossy for
# the collapsed gears; the reverse lookup always returns the canonical
# representative, not the original sub-type.
gear_mapping <-
  tribble(
   ~version,    ~gid,         ~veiðarfæri,  ~map,  ~gear, ~target,
      "new",       1,      "Skötuselsnet",    91,   "GNS",  "DEF",
      "new",       2,      "Þorskfisknet",     2,   "GNS",  "DEF",
      "new",       3,      "Grásleppunet",    25,   "GNS",  "DEF",
      "new",       4,       "Rauðmaganet",    29,   "GNS",  "DEF",
      "new",       5,            "Reknet",    11,   "GND",  "SPF",
      "new",       6,         "Botnvarpa",     6,   "OTB",  "DEF",
      "new",       7,        "Humarvarpa",     9,   "OTB",  "CRU",
      "new",       8,        "Rækjuvarpa",    14,   "OTB",  "CRU",
      "new",       9,         "Flotvarpa",     7,   "OTM",  "SPF",
      "new",      10,              "Nót",     12,    "PS",  "SPF",
      "new",      11,          "Dragnót",      5,   "SDN",  "DEF",
      "new",      12,             "Lína",      1,   "LLS",  "DEF",
      "new",      13,   "Landbeitt lína",      1,   "LLS",  "DEF",
      "new",      14,         "Handfæri",      3,   "LHM",  "DEF",
      "new",      15,           "Plógur",     15,   "DRB",  "DES",
      "new",      16,           "Gildra",     17,   "FPO",  "DEF",
      "new",      17,     "Annað - Hvað",     99,   "MIS",  "DWF",
      "new",      18,         "Eldiskví",     NA,      NA,     NA,
      "new",      19,         "Sjóstöng",     45,   "LHP",  "FIF",
      "new",      20,    "Kræklingalína",     42,      NA,     NA,
      "new",      21,        "Línutrekt",      1,   "LLS",  "DEF",
      "new",      22,       "Grálúðunet",     92,   "GNS",  "DEF",
      "new",      23,           "Kafari",     41,   "DIV",  "DES",
      "new",      24,     "Sláttuprammi",     NA,   "HMS",  "SWD",
      "new",      25,       "Þaraplógur",     NA,   "HMS",  "SWD",
      "old",       1,             "Lína",     13,   "LLS",  "DEF",
      "old",       2,              "Net",      2,   "GNS",  "DEF",
      "old",       3,         "Handfæri",     14,   "LHM",  "DEF",
      "old",       4,         "Þorsknót",     NA,      NA,     NA,
      "old",       5,   "Dragnót 135 mm",     11,    "SDN", "DEF",
      "old",       6,        "Botnvarpa",      6,   "OTB",  "DEF",
      "old",       7,        "Flotvarpa",      9,   "OTM",  "SPF",
      "old",       8,   "Spærlingsvarpa",      9,   "OTM",  "SPF",
      "old",       9,       "Humarvarpa",      7,   "OTB",  "CRU",
      "old",      10,        "Síldarnót",     10,    "PS",  "SPF",
      "old",      11,           "Reknet",      5,   "GND",  "SPF",
      "old",      12,         "Loðnunót",     10,    "PS",  "SPF",
      "old",      13,    "Loðnuflotvarpa",     9,   "OTM",  "SPF",
      "old",      14,        "Rækjuvarpa",     8,   "OTB",  "CRU",
      "old",      15,   "Hörpudiskplógur",    15,   "DRB",  "MOL",
      "old",      16,       "Grálúðulína",    12,   "LLS",  "DEF",
      "old",      17,       "Þorskgildra",    16,   "FPO",  "DEF",
      "old",      18,      "Krabbagildra",    16,   "FPO",  "CRU",
      "old",      19,     "Gulllaxavarpa",     9,   "OTM",  "SPF",
      "old",      20,   "Ýmis veiðarfæri",    17,   "MIS",  "DWF",
      "old",      21,  "Síldar-/kolmunnaflotvarpa",     9,   "OTM",  "SPF",
      "old",      25,      "Grásleppunet",     3,   "GNS",  "DEF",
      "old",      26,    "Dragnót 120 mm",    11,   "SDN",  "DEF",
      "old",      29,       "Rauðmaganet",     4,   "GNS",  "DEF",
      "old",      35,    "Dragnót 155 mm",    11,    "SDN", "DEF",
      "old",      38,    "Kúffisksplógur",    15,   "DRB",  "MOL",
      "old",      39,  "Beitukóngsgildra",    16,   "FPO",  "MOL",
      "old",      40,    "Ígulkeraplógur",    15,   "DRB",  "DES",
      "old",      41,    "Ígulkerakafari",    23,   "DIV",  "DES",
      "old",      42,     "Kræklingalína",    20,      NA,     NA,
      "old",      45,          "Sjóstöng",    19,    "LHP",  "FIF",
      "old",      91,       "Skötuselsnet",    1,    "GNS",  "DEF",
      "old",      92,         "Grálúðunet",   22,    "GNS",  "DEF",
      "old",      99,  "Óskráð veiðarfæri",   17,    "MIS",  "DWF") |>
  mutate(gid = as.integer(gid),
         map = as.integer(map))

# check validity ---------------------------------------------------------------
gear_mapping |>
  select(gear, target) |>
  distinct() |>
  filter(!is.na(gear)) |>
  mutate(
    v_gear =
      case_when(gear %in% icesVocab::getCodeList("GearType")$Key ~ TRUE,
                .default = FALSE),
    v_target =
      case_when(target %in% icesVocab::getCodeList("TargetAssemblage")$Key ~ TRUE,
                .default = FALSE),
    v_met5 =
      case_when(paste0(gear, "_", target) %in% icesVocab::getCodeList("Metier5_FishingActivity")$Key ~ TRUE,
                .default = FALSE)
    #v_met6 =
    #  case_when(met6 %in% icesVocab::getCodeList("Metier6_FishingActivity")$Key ~ TRUE,
    #            .default = FALSE)
  ) |>
  filter(!v_gear | !v_target | !v_met5) |>
  knitr::kable(caption = "Problems with gear and target not in ICES vocabulary, but met5 and met6 considered valid.")
gear_mapping |>
  nanoparquet::write_parquet("data/gear/gear_mapping.parquet")
# check: have I captured all gears in the logbook data? ------------------------
read_parquet("data-raw/data-dump/afli/stofn.parquet") |>
  count(gid = veidarf) |>
  left_join(gear_mapping |> filter(version == "old")) |>
  knitr::kable()
read_parquet("data-raw/data-dump/fs_afladagbok/ws_veidi.parquet") |>
  count(gid = veidarfaeri_id) |>
  left_join(gear_mapping |> filter(version == "new")) |>
  knitr::kable()

