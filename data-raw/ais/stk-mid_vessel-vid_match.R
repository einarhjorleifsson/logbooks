# Classify the mobileid-localid-globalid in the stk-data
#  Focus on vessel data
#   * Assign MMSI, if possible to all Icelandic vessels
#   * Attempt to assign MMSI to foreign vessels
#   * Attempt to write the code flow so it can easily be updated
#     once new data are compiled (Not there yet)
#
#  The code includes many "tests", intended to capture possible match issues
#   as upstream as possible. The code is a result of iterative procedure where
#   overlaps (same mobileid on 2-3 vessels) are reported in the dual spreadsheet,
#   then whole process run a again, checking again for issues at each step.

# TODO: ------------------------------------------------------------------------
#   MOVE ALL MANUAL MATCHING TO THE DUAL DOCUMENT - done
#   VID 3700-4999: these are foreign vessels — no further matching effort planned
#   UNMATCHED ICELANDIC MMSI (genuine vessels, not in registry — need Fiskistofa lookup):
#     mid 114902 MMSI 251950001 (active 2015-16, 2024+)
#     mid 118765 MMSI 251068418 (active 2015)
#     mid 139879 MMSI 251898641 (active 2021-2023)
#   ODS DATE ERROR: mid 101097 (vid 1328) — d1/d2 in ODS are both in 2008 (~15-day window)
#     but mid is active 2007-2026; no=2 (vid 3025) starts 2023-09-07.
#     Correct ODS no=1 to: d1=2007-06-01, d2=2023-09-06
# NEWS
# 202X-12-31
#
# 2024-10-24
#  * consolidation, review and addition of older matches
# INPUT
#    oracle stk.trail
#    oracle stk.mobile
#    data/auxillary/maritime_identification_digits.parquet
#    data/vessels/vessels_iceland.parquet
#    older matches of mobileid and vessel registration number
#     see: ~/R/Pakkar2/omar/data-raw/00_SETUP_mobile-vid-match.R
#          last updated 2024-02-12
#          ops$einarhj.mobile_vid
#
# OUTPUT
# data-raw/stk-mmsi-vessel-match/******.parquet

library(nanoparquet)
library(duckdbfs)
library(tidyverse)

options(knitr.kable.NA = '')
#library(mar)

#con <- connect_mar()

# a stk summary helper function
lh <- function(d) {
  d |>
    group_by(.loid, .glid) |>
    reframe(n = n(), pings = sum(pings)) |>
    arrange(-n) |>
    mutate(cn = round(cumsum(n / sum(n)) * 100, 2),
           .after = n) |>
    mutate(pp = round(pings / sum(pings) * 100, 2)) |>
    knitr::kable()
}
lh_speed <- function(MID, trim = TRUE) {
  d <-
    STK |>
    filter(mid %in% MID) |>
    select(mid, time, speed) |>
    collect() |>
    mutate(speed = ifelse(speed > 15, 15, speed))
  if(trim) {
    d <- d |> filter(time >= ymd_hms("2007-06-01 00:00:00"))
  }
  d |>
    ggplot(aes(time, speed)) + geom_point(size = 0.1) +
    facet_wrap(~ mid, ncol = 1) +
    scale_x_datetime(date_breaks = "2 year", date_labels = "%Y")
}
lh_overlaps <- function(d) {
  d |>
    group_by(vid) |>
    mutate(n = n()) |>
    ungroup() |>
    filter(n > 1, !is.na(vid)) |>
    arrange(vid, d2) |>
    group_by(vid) |>
    mutate(dt = difftime(lead(d1), d2, units = "day"),
           dt = as.numeric(dt)) |>
    mutate(overlap = case_when(dt < -1 ~ "yes",
                               .default = "no")) |>
    filter(overlap == "yes") |>
    pull(vid) |>
    unique() ->
    vid_overlap
  d |>
    filter(vid %in% vid_overlap) |>
    arrange(vid, d2) |>
    group_by(vid) |>
    mutate(overlap = case_when(d2 > lead(d1) ~ "yes",
                               .default = "no"))
}
lh_overlaps_plot <- function(MID, stkX) {
  STK |>
    filter(mid %in% MID) |>  # Need to check this
    select(mid, loid, glid, time, speed) |>
    mutate(speed = ifelse(speed > 15, 15, speed)) |>
    collect() |>
    left_join(stkX,
              by = join_by(mid == mid, loid == loid, glid == glid,
                           between(time, d1, d2))) |>
    ggplot(aes(time, speed, colour = factor(mid))) +
    geom_point(size = 0.01) +
    facet_wrap(~ vid, ncol = 1) +
    scale_x_datetime(date_breaks = "2 year", date_labels = "%Y")
}

lh_mmsi_expectations <- function(d) {
  d |>
    filter(vid > 0) |>
    #filter(step != "duals") |>
    left_join(v_all |> select(vid, mmsi, yh1, yh2)) |>
    filter(is.na(mmsi)) |>
    arrange(desc(d2)) |>
    knitr::kable(caption = "Don't expect missing mmsi for 'recent' d2")
}


# Check that it doesn't match any non-letter
letters_only <- function(x) !grepl("[^A-Za-z]", x)
# Check that it doesn't match any non-number
numbers_only <- function(x) !grepl("\\D", x)

str_extract_between_parenthesis <- function(x) {
  str_match(x, "(?<=\\().+?(?=\\))")
}

str_extract_dmy_period <-
  function(x) {
    str_extract(x, "\\d+\\D+\\d+\\D+\\d+")
  }

# Data -------------------------------------------------------------------------

## Various auxillary informations ----------------------------------------------
## Maritime identification digits
MID <-
  read_parquet("data/aux/maritime_identification_digits.parquet") |>
  # used for classifying likely incomplete mmsi-signals
  mutate(MID_child = paste0("98", MID),
         MID_aid = paste0("99", MID))
## Call sign prefix - flag state
cs.prefix <-
  read_parquet("data/aux/callsign_prefix.parquet") |>
  # critical, lot of mess with TF in stk localid and globalid
  filter(cs_prefix != "TF")
## vessel registry
#  should really add skemmtibatar
v_all <-
  read_parquet("data/vessels/vessels_iceland.parquet") |>
  mutate(vidc = as.character(vid))
v_mmsi <-
  v_all |>
  filter(!is.na(mmsi))

## Known (via ad hoc) non-vessel localid or globalid ---------------------------
fixed <-
  c("Surtseyja", "Straumnes", "Steinanes", "Haganes_K", "Bakkafjar",
    "Laugardal", "BorgfjE P", "Gemlufall", "Straumduf", "Eyrarhlid",
    "Hvalnes", "Straumm?l", "V_Blafj P", "Isafj.dju", "Rey?arfjo",
    "VidarfjAI", "KLIF AIS",  "VadlahAIS", "Hafell_AI", "TIND_AIS",  "Storhof?i",
    "Helguv", "Laugarb.f", "Grimseyja", "G.skagi",   "Grindavik", "Hornafjar",
    "Flateyjar", "Kogurdufl", "Blakkur.d", "Bakkafjor", "Hvalbakur", "SUGANDI_A",
    "TJALDANES",  "Snaefj1",
    "Snaefj2", "Lande", "Sjomsk", "TJALD.NES", "illvid_p", "BLAKKSNES", "V_Sfell B",
    "HOF", "Illvi?rah", "Miðfegg P", "BASE11", "Borgarfj ",
    "V_Hofsos", "V_Hofsos ", "Arnarfjor", "Trackw", "SUGANDAFJ",
    "BORGARÍS", "BORGARIS", "BORGARIS0", "BORGARIS1",
    "ThverfAIS",
    "TEST",
    "Hvannadal", "Tjaldanes", "BorglAIS", "HvalnAIS", "Midf_AIS",
    "Hellish A", "GreyAIS", "Berufjor?",
    "Baeir", "Frodarh_A", "Onundarfj", "HusavikAI", "Haukadals",
    "Drangsnes", "Hofdahusa", "Djupiv-AI", "Dyrafjor?", "Faskru?sf",
    "Fossfjor?", "Hvestudal", "Hringsdal", "Bakkafj-d", "Mulagong",
    "Grnipa P", "Haenuvk P", "Bolafj P", "Ennish P", "Grimsey P",
    "Frodarh P", "Haoxl B", "Hafell P", "Vidarfj P", "Djupiv P",
    "Blafj P", "Sigmundar", "Tjnes P", "Sfell P", "Hellish P",
    "Gvkurfj P", "Klif P", "Thverfj B", "Klif B", "Grimsey B",
    "Frodarh B", "Hvalnes P", "Haoxl P", "Grnipa B", "Illvidh P",
    "FLATEYRI_", "Hellish B", "Husavik B", "Hofsos P", "Faskra?sf",
    "Husavik P", "Tjornes P", "Thorbj B", "Borgarh-P", "Baeir B",
    "VadlahP", "Thverfj P", "Dalvik P", "Godat-P", "HafellAIS",
    "Bjolfur P", "Ennish B", "Thorbj P", "Hraunh P", "Gufusk P",
    "Lambhgi P", "Fri?rik A", "Baeir P", "Flatey du", "Fellsgg1P",
    "Fellsgg2P", "Akurtr B", "Midfell-P", "Horgargru", "Borgarl P",
    "Haenuvk B", "Gagnhdi P", "Hvalnes B", "HVestudal", "Gildruh B",
    "Sfell B", "Gagnhdi B", "BorgfjE B", "Spolur-P", "Klakkur P",
    "KOLBEINSE", "Stykkh P", "Tjnes B", "Kvigindis", "Dufl_GRV_",
    "Fell P", "Steinny-P", "Stokksn P", "Tjorn P", "Kopasker",
    "Akreyri P", "Grima P", "Dalatgi B", "ThverfjP", "Rifssker_",
    "Dalatgi P", "Tjorn B", "Kolmuli_K", "Vattarnes", "Thorbjorn",
    "Husavik", "Hafranes_", "Drangaj_P", "Hrisey", "Hofsos",
    "Midfegg P", "Midf P", "Gufunes P", "Mi?fegg P", "Dalvík P",
    "Dalvik", "Borgfj E", "Straumn-A", "Talknaf P",
    "Steinny", "TILK", "ThverfjP1", "Heidar-P", "Vadlaheid",
    "Talknaf B", "BLAKK_AIS", "Mork-P", "VPN_Bauja",
    "PF7567", "Daltat", "AEDEY AIS", "Borgfj E",
    # should really use the mobile id here, at least it is easier
    #  because that is how things are checked iteratively
    "2515036", "2311200", "2311400", "2573900", "2311500",
    "2515071", "25150051", "2314000",
    "251510120",    # Skipstjóraskólinn
    "231140005",
    "231140003",
    "251999898",
    "231140004",
    "231140006",
    "231140001",
    "231140002",
    "251513130",
    "xxx5",
    "103984",
    "Borgfj E",
    "Borgfj E ",
    "BLAKK_OLD",
    "Gufunes B",
    "Blondos P",
    "Mork P",
    "Va?lahei?",
    # Icelandic MMSI but non-vessel pattern (AIS-SART / buoy / special)
    "251999895",  # mid 121326 - speed ~0 throughout, scattered pings
    "251990271"   # mid 126636 - brief 2017 coastal activity, 99x prefix
  )
kvi <-
  c("Eyri_Kvi_", "Kvi_Skotu", "Kvi_Baeja", "Bjarg_Kvi", "Sjokvi-4", "Sjokvi-3",
    "Kvi-0 Hri", "Sjokvi-2", "Kvi_Sande", "Kvi_Fenri", "Sjokvi",
    "Y.Kofrady")
hafro <-
  c("Hafro_Str", "Hafro_O2_", "Hafro_CO2", "Hafro_H11", "Hafro_H20", "Hafro_Hva",
    "Hafro_duf", "AfmHafro_", "Afm_Hafro", "Hafro_W.V", "Hafro_W.V ", "afm_Hafro",
    "Rannsokn_")
net_glid <-
  c("9911378")
unknown_glid <- c("5200000")

## Vessel info for local- and globalid classification --------------------------
# suffix 2 is when mmsi not available
CS <-
  v_mmsi |>
  filter(!is.na(cs)) |>
  filter(nchar(cs) %in% 4:7) |>
  pull(cs) |>
  unique()
CS2 <-
  v_all |>
  filter(!vid %in% 3700:4999) |>
  filter(!is.na(cs)) |>
  filter(nchar(cs) %in% 4:7) |>
  pull(cs) |>
  unique()
CS3 <-
  v_all |>
  filter(vid %in% 3700:4999) |>
  filter(!is.na(cs)) |>
  filter(nchar(cs) %in% 4:7) |>
  pull(cs) |>
  unique()
UID <-
  v_mmsi |>
  filter(!is.na(uid)) |>
  pull(uid) |>
  unique()
UID2 <-
  v_all |>
  filter(!is.na(uid)) |>
  pull(uid) |>
  unique()
VID <-
  v_mmsi |>
  filter(source == "ISL", !is.na(vid)) |>
  pull(vid) |>
  as.character()
VID2 <-
  v_all |>
  filter(source == "ISL", !is.na(vid)) |>
  pull(vid) |>
  as.character()

lnd <-
  read_parquet("data-dump/landings/agf/aflagrunnur.parquet") |>
  #read_parquet("data/landings/agf_stations.parquet") |>
  group_by(vid = skip_numer) |>
  reframe(n = n(),
          min = min(londun_hefst),
          max = max(londun_hefst)) |>
  filter(vid >= 5)

## stk summary -----------------------------------------------------------------
STK <-
  open_dataset("data-dump/ais/stk/trail")
mobile <- open_dataset("data-dump/ais/stk/mobile.parquet")
stk <-
  STK |>
  group_by(mid, year) |>
  mutate(n = n()) |>
  ungroup() |>
  mutate(n = n()) |>
  filter(n >= 10) |>
  left_join(mobile) |>
  group_by(mid, loid, glid) |>
  summarise(pings = n(),
            n_years = n_distinct(year),
            d1 = min(time, na.rm = TRUE),
            d2 = max(time, na.rm = TRUE),
            .groups = "drop") |>
  collect() |>
  # these used when using join with between
  mutate(d1 = as_date(d1),
         d2 = as_date(d2) + ddays(1)) |>  # rather than 23:59:59 if time was used
  # this however includes the first second of next day
  arrange(mid, d1, d2) |>
  mutate(.rid = 1:n(),
         .before = mid)

# Older matches ----------------------------------------------------------------
older <-
  read_parquet("data-dump/ais/stk/einarhj_mobile-vid.parquet") |>
  select(mid:vid, no, t1, t2) |>
  # add loid and glid where missing
  left_join(stk |> select(mid, loid_tmp = loid, glid_tmp = glid, d1, d2),
            by = join_by(mid)) |>
  mutate(loid = ifelse(is.na(loid), loid_tmp, loid),
         glid = ifelse(is.na(glid), glid_tmp, glid)) |>
  select(-c(loid_tmp, glid_tmp)) |>
  arrange(mid) |>
  group_by(mid) |>
  mutate(n.mid = n()) |>
  ungroup()

# Join manual (duals) ----------------------------------------------------------
manual <-
  "data-dump/ais/stk/stk_mobile_match-and-duals.ods" |>
  #"data-raw/stk_mobile_match-and-duals/stk_mobile_match-and-duals.ods" |>
  readODS::read_ods() |>
  select(mid:d2) |>
  # Guard: fix swapped d1/d2 (ODS date-entry errors)
  mutate(d1_tmp = pmin(d1, d2, na.rm = TRUE),
         d2     = pmax(d1, d2, na.rm = TRUE),
         d1     = d1_tmp) |>
  select(-d1_tmp)

stk1 <-
  stk |>
  left_join(manual |>
              select(mid, vid, no, .d1 = d1, .d2 = d2)) |>
  mutate(d1 = case_when(!is.na(.d1) ~ .d1,
                        .default = d1),
         d2 = case_when(!is.na(.d2) ~ .d2,
                        .default = d2)) |>
  select(-c(.d1, .d2)) |>
  mutate(step = case_when(!is.na(vid) ~ "manuals",
                          .default = NA))


# Classification ---------------------------------------------------------------
stk2 <-
  stk1 |>
  # The order matters in the case_when
  mutate(.loid =
           case_when(
             loid %in% fixed ~ "fixed",
             loid %in% kvi ~ "kvi",
             loid %in% VID ~ "vid",
             loid %in% VID2 ~ "vid2",
             #
             loid %in% CS ~ "cs",
             loid %in% CS2 ~ "cs2",
             loid %in% CS3 ~ "cs3",
             loid %in% UID ~ "uid",
             loid %in% UID2 ~ "uid2",
             str_sub(loid, 1, 2) %in% cs.prefix$cs_prefix &
               !numbers_only(str_trim(loid)) &
               !str_starts(loid, "MOB_")  ~ "cs4",
             numbers_only(loid) & str_sub(loid, 1, 5) %in% MID$MID_child ~ "mmsi.other",
             numbers_only(loid) & str_sub(loid, 1, 5) %in% MID$MID_aid ~ "mmsi.other",
             numbers_only(loid) & nchar(loid) == 9  ~ "mmsi",
             .default = NA)) |>
  mutate(.glid =
           case_when(
             glid %in% fixed ~ "fixed",
             glid %in% kvi ~ "kvi",
             glid %in% hafro ~ "hafro",
             glid %in% unknown_glid ~ "unknown",
             str_detect(tolower(glid), "_net") ~ "net",
             glid %in% net_glid ~ "net",
             glid %in% VID ~ "vid",
             glid %in% VID2 ~ "vid2",
             # numbers_only(glid) & nchar(glid) == 9 ~ "mmsi",
             glid %in% CS ~ "cs",
             glid %in% CS2 ~ "cs2",
             glid %in% CS3 ~ "cs3",
             glid %in% UID ~ "uid",
             glid %in% UID2 ~ "uid2",
             str_sub(glid, 1, 2) %in% cs.prefix$cs_prefix &
               !numbers_only(str_trim(glid)) &
               !str_starts(glid, "MOB_")  ~ "cs4",
             numbers_only(glid) & str_sub(glid, 1, 5) %in% MID$MID_child ~ "mmsi.other",
             numbers_only(glid) & str_sub(glid, 1, 5) %in% MID$MID_aid ~ "mmsi.other",
             numbers_only(glid) & nchar(glid) == 9 ~ "mmsi",
             #numbers_only(glid) & nchar(glid) == 7 & !omar::vessel_valid_imo(glid) ~ "imo",
             .default = NA)) |>
  mutate(type = paste0(replace_na(.loid, "NA"), "_", replace_na(.glid, "NA"))) |>
  select(-c(.loid, .glid))

stk2 |> lh_overlaps() |> filter(vid > 0) |>  knitr::kable(caption = "Expect none")

# Join older -------------------------------------------------------------------
#  Note: the filter(n.mid == 1) filters out all but one of the older vid 3700 to 4999
stk3 <-
  stk2 |>
  left_join(older |> filter(n.mid == 1) |> select(mid, vid_older = vid))
stk3 |> lh_overlaps() |> filter(vid > 0) |> knitr::kable(caption = "Expect none")
stk3 |> filter(!vid %in% 3700:4999) |> lh_mmsi_expectations()

## vid_vid ---------------------------------------------------------------------
stk4 <-
  stk3 |>
  mutate(vid = case_when(!is.na(vid) ~ vid,
                         type == "vid_vid" & loid == glid ~ as.numeric(glid),
                         .default = vid)) |>
  mutate(step = case_when(!is.na(vid) & is.na(step) ~ "vid_vid",
                          .default = step))
stk4 |>
  filter(type == "vid_vid") |>
  filter(is.na(vid)) |>
  knitr::kable(caption = "Missing vid for type vid_vid (expect none)")

# duals with overlaps
stk4 |> lh_overlaps() |> filter(vid > 0) |> knitr::kable(caption = "Expect vid 1511, 7807")
stk4 |> lh_mmsi_expectations()

## vid_cs match ----------------------------------------------------------------
stk5 <-
  stk4 |>
  mutate(.vid = case_when(type == "vid_cs" ~ as.numeric(loid),
                          .default = -9999),
         .cs = case_when(type == "vid_cs" ~ glid,
                         .default = "XXXX")) |>
  left_join(v_mmsi |> select(.vid = vid, .cs = cs) |> mutate(type = "vid_cs"),
            by = join_by(.vid, .cs, type)) |>
  mutate(vid = case_when(!is.na(vid) ~ vid,
                         type == "vid_cs" & vid_older == .vid & glid == .cs ~ vid_older,
                         type == "vid_cs" & is.na(vid_older) ~ .vid,
                         type == "vid_cs" & vid_older == 6643 ~ vid_older,  # single case
                         .default = vid)) |>
  select(-c(.vid, .cs)) |>
  mutate(step = case_when(!is.na(vid) & is.na(step) ~ "vid_cs",
                          .default = step))
stk5 |>
  filter(type == "vid_cs") |>
  filter(is.na(vid)) |>
  knitr::kable(caption = "Missing vid for type vid_cs (expect none)")

stk5 |> filter(vid > 0) |> lh_overlaps() |> knitr::kable(caption = "Expect vid 1511, 2718, 7807")
if(FALSE) {
  lh_overlaps_plot(c(103851, 101223), stk5) # expected, vid 2718 has one mid inbetween another mid
}
stk5 |> filter(!vid %in% 3700:4999) |>  lh_mmsi_expectations()
## uid_cs match ----------------------------------------------------------------
stk6 <-
  stk5 |>
  mutate(.uid = case_when(type == "uid_cs" ~ loid,
                          .default = "XXXX"),
         .cs = case_when(type == "uid_cs" ~ glid,
                         .default = "XXXX")) |>
  left_join(v_mmsi |> select(.vid = vid, .cs = cs, .uid = uid) |> mutate(type = "uid_cs"),
            by = join_by(.cs, .uid, type)) |>
  mutate(vid = case_when(!is.na(vid) ~ vid,
                         type == "uid_cs" & vid_older == .vid ~ vid_older,
                         type == "uid_cs" & !is.na(vid_older) ~ vid_older,
                         .default = vid)) |>
  select(-c(.cs, .uid, .vid)) |>
  mutate(step = case_when(!is.na(vid) & is.na(step) ~ "uid_cs",
                          .default = step))
stk6 |>
  filter(type == "uid_cs") |>
  filter(is.na(vid)) |>
  knitr::kable(caption = "Missing vid for type uid_cs (expect none)")
stk6 |> filter(vid > 0) |> lh_overlaps() |> knitr::kable(caption = "Expect vid 1511, 2718, 7807")
stk6 |> filter(!vid %in% 3700:4999) |> lh_mmsi_expectations()

## NA_vid match ----------------------------------------------------------------
stk7 <-
  stk6 |>
  mutate(.vid = case_when(type == "NA_vid" ~ glid,
                          .default = NA)) |>
  left_join(v_mmsi |> select(.vid = vidc, mmsi) |> mutate(type = "NA_vid"),
            by = join_by(.vid, type)) |>
  mutate(vid = case_when(!is.na(vid) ~ vid,
                         type == "NA_vid" & !is.na(mmsi) ~ as.integer(.vid),
                         .default = vid)) |>
  select(-c(.vid, mmsi)) |>
  mutate(step = case_when(!is.na(vid) & is.na(step) ~ "NA_vid",
                          .default = step))
stk7 |>
  filter(type == "NA_vid") |>
  filter(is.na(vid)) |>
  knitr::kable(caption = "Missing vid for type NA_vid (expect none)")

stk7 |> filter(vid > 0) |> lh_overlaps() |> knitr::kable(caption = "Expect vid 1511, 2718, 7807")
if(FALSE) {
  lh_overlaps_plot(c(101081), stk7) # Still dubious, second period for 2067 may be a wrong allocation
}
stk7 |> filter(!vid %in% 3700:4999) |> lh_mmsi_expectations()

## NA_cs ------------------------------------------------------------------------
stk8 <-
  stk7 |>
  mutate(.cs = glid) |>
  left_join(v_mmsi |> select(.cs = cs, .vid = vid, .uid = uid, yh1, yh2) |> mutate(type = "NA_cs")) |>
  mutate(vid = case_when(!is.na(vid) ~ vid,
                         type == "NA_cs" & vid_older == .vid  ~ vid_older,
                         type == "NA_cs" & !is.na(.vid) ~ .vid,
                         .default = vid)) |>
  select(-c(.cs, .vid, .uid, yh1, yh2)) |>
  mutate(step = case_when(!is.na(vid) & is.na(step) ~ "NA_csts",
                          .default = step))
stk8 |>
  filter(type == "NA_cs") |>
  filter(is.na(vid)) |>
  knitr::kable(caption = "Missing vid for type NA_cs (expect none)")
stk8 |> filter(vid > 0) |> lh_overlaps() |> knitr::kable(caption = "Expect vid 1511, 2718, 7807")
stk8 |> filter(!vid %in% 3700:4999) |> lh_mmsi_expectations()

## vid2_cs ---------------------------------------------------------------------
# Question if this should come next
stk9 <-
  stk8 |>
  mutate(.cs = glid) |>
  left_join(v_mmsi |>
              select(.cs = cs, .vid = vidc, .uid = uid, yh1, yh2) |> mutate(type = "vid2_cs") |>
              # did this to not get many-to-many
              filter(yh2 > 2013)) |>
  mutate(vid = case_when(!is.na(vid) ~ vid,
                         type == "vid2_cs" ~ as.numeric(.vid),
                         .default = vid)) |>
  select(-c(.cs, .vid, .uid, yh1, yh2)) |>
  mutate(step = case_when(!is.na(vid) & is.na(step) ~ "vid2_cs",
                          .default = step))
stk9 |>
  filter(type == "vid2_cs") |>
  filter(is.na(vid)) |>
  knitr::kable()
stk9 |> filter(vid > 0) |> lh_overlaps() |> knitr::kable(caption = "Expect vid 1511, 2718, 7807")
stk9 |> filter(!vid %in% 3700:4999) |> lh_mmsi_expectations()


if(FALSE) {
  lnd |>
    filter(!vid %in% c(stk9 |> select(vid) |> drop_na() |> pull(vid))) |>
    arrange(desc(max)) |>
    left_join(v_all) |>
    knitr::kable(caption = "List of vessels in landings not in stk")

  # CHECK THIS
  v_all |> filter(vid == 3027)
  stk9 |> filter(vid == 1115)
  v_all |> filter(cs == "TFAU")
  v_all |> filter(uid == "RE245")
  v_all |> filter(uid == "GK011")
}

## vid2_vid2 -------------------------------------------------------------------
# When loid == glid and both classify as vid2, the vid is encoded directly in
# loid/glid — same logic as stk4's vid_vid step but for vessels without MMSI
stk9b <-
  stk9 |>
  mutate(vid = case_when(!is.na(vid) ~ vid,
                         type == "vid2_vid2" & loid == glid ~ as.numeric(loid),
                         .default = vid)) |>
  mutate(step = case_when(!is.na(vid) & is.na(step) ~ "vid2_vid2",
                          .default = step))
stk9b |>
  filter(type == "vid2_vid2") |>
  filter(is.na(vid)) |>
  knitr::kable(caption = "Missing vid for type vid2_vid2 where loid==glid (expect none for loid==glid rows)")
stk9b |> filter(vid > 0) |> lh_overlaps() |> knitr::kable(caption = "Expect vid 1511, 2718, 7807")
stk9b |> filter(!vid %in% 3700:4999) |> lh_mmsi_expectations()

## VID_OLDER -------------------------------------------------------------------
# hail mary, step may be premature
stk10 <-
  stk9b |>
  mutate(vid = case_when(!is.na(vid) ~ vid,
                         is.na(vid) & !is.na(vid_older) & pings > 100 ~ vid_older,
                         .default = vid)) |>
  mutate(step = case_when(!is.na(vid) & is.na(step) & pings > 100 ~ "older",
                          .default = step))

stk10 |> filter(vid > 0) |> lh_overlaps() |> knitr::kable(caption = "Expect vid 396, 950, 1511, 2718, 7807")
if(FALSE) {
  lh_overlaps_plot(c(113343, 107082), stk10) # 396
  lh_overlaps_plot(c(107945, 100430), stk10) # 950
}
stk10 |> filter(!vid %in% 3700:4999) |> lh_mmsi_expectations()

## NA_mmsi ---------------------------------------------------------------------
v_mmsi_no_dupes <-
  v_mmsi |>
  arrange(mmsi, desc(mmsi_t2), desc(vid)) |>
  group_by(mmsi) |>
  slice(1) |>
  ungroup()
stk11 <-
  stk10 |>
  mutate(.mmsi = glid) |>
  left_join(v_mmsi_no_dupes |> select(.vid = vidc, .mmsi = mmsi, yh1, yh2) |> mutate(type = "NA_mmsi")) |>
  mutate(vid = case_when(!is.na(vid) ~ vid,
                         type == "NA_mmsi" & pings >= 30 ~ as.numeric(.vid),
                         .default = vid)) |>
  select(-c(.vid, .mmsi, yh1, yh2)) |>
  mutate(step = case_when(!is.na(vid) & is.na(step) ~ "NA_mmsi",
                          .default = step))
stk11 |>
  filter(type == "NA_mmsi") |>
  filter(!is.na(vid)) |>
  pull(vid) ->
  vids
stk11 |>
  filter(vid %in% vids) |>
  group_by(vid) |>
  mutate(n = n()) |>
  ungroup() |>
  filter(n > 1) |>
  arrange(vid, d2)
if(FALSE) {
  lh_overlaps_plot(c(120568, 100873), stk11)
  lh_overlaps_plot(c(143787, 102969), stk11)
}

# I AM HERE --------------------------------------------------------------------

lnd |>
  filter(vid < 9900) |>
  filter(!vid %in% c(stk11 |> select(vid) |> drop_na() |> pull(vid))) |>
  arrange(desc(max)) |>
  left_join(v_all) |>
  knitr::kable(caption = "List of vessels in landings not in stk")

# Add MMSI to Icelandic vessels ------------------------------------------------
stk12 <-
  stk11 |>
  left_join(v_mmsi_no_dupes |> select(vid, mmsi))

# Extend the last date ---------------------------------------------------------
# Need to extend the 'last' "d2" within any mobileid to something like 2028-12-24
#  This is so that 'later' stk dumps get included in the between join downstream
stk13 <-
  stk12 |>
  mutate(no = replace_na(no, 1)) |>
  group_by(mid) |>
  mutate(d2 = case_when(no == max(no) ~ ymd("2028-12-24"),
                        .default = d2)) |>
  ungroup()
# SAVE THE STUFF ---------------------------------------------------------------
stk13 |>
  mutate(pings = as.integer(pings),
         n_years = as.integer(n_years),
         vid = as.integer(vid),
         no = as.integer(no),
         vid_older = as.integer(vid_older)) |>
  nanoparquet::write_parquet("data/vessels/stk_vessel_match.parquet")
