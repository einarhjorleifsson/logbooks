# https://gitlab.hafogvatn.is/dag/00-setup/-/blob/master/logbooks/catch.R

## db setup
## stuff to load for the first time..
library(tidyverse)
library(duckdbfs)
tyr <- lubridate::year(Sys.Date())


## Compiles available catch data, mainly for presentational purposes


try(dbRemoveTable(mar,'logbooks_compiled'))
## HACK: fixes recorded depth
if(FALSE){
  gr <-
    mar %>%
    afli_stofn() %>%
    filter(ar %in% c(2007, 2008, 2009, 2013, 2014, 2015)) %>%
    select(visir, ar, lon = lengd, lat = breidd,
           gid = veidarf,
           dypi) %>%
    encode_zchords(dx = 0.125,dy = 0.125/2) %>%
    filter(gid %in% c(5,26,27,35,6,59,60,61,62,63)) %>%
    group_by(sq) %>%
    summarise(m = mean(dypi)) %>%
    filter(m>0) %>% collect(n=Inf)

  dbWriteTable(mar,'gr_tmp',gr)
  dbExecute(mar, 'grant select on "gr_tmp" to public')
}

mar::afli_afli(mar) %>%
  dplyr::left_join(afli_afli(mar) %>%
                     dplyr::group_by(visir) %>%
                     dplyr::summarise(total=sum(afli,na.rm = TRUE))) %>%
  dplyr::inner_join(mar::afli_stofn(mar) %>%
                      mutate(gridcell = reitur*10+smareitur)) %>%
  dplyr::left_join(mar::afli_toga(mar) %>%
                     dplyr::select(visir,togtimi,gear_size = staerd)) %>%
  dplyr::left_join(mar::afli_lineha(mar) %>%
                     dplyr::mutate(hooks = onglar*bjod, nr_net = dregin) %>%
                     dplyr::select(visir,hooks, nr_net)) %>%
  dplyr::left_join(tbl_mar(mar,'afli.gildra')) %>%
  dplyr::left_join(tbl(mar,'gear_mapping'),by=c('veidarf'='veidarfaeri')) %>%
  dplyr::mutate(togtimi = case_when(is.na(klst)~togtimi,
                                    TRUE~klst),
                num_traps = case_when(!is.na(gildrur)~gildrur,
                                      veidarf %in% c(17, 18, 39, 51, 52) ~ toglengd/5,
                                      TRUE~gildrur)) %>%
  dplyr::select(id=visir,species = tegund,towtime=togtimi,gear,vessel_nr=skipnr,year=ar,month=man,
                lat=breidd,lon=lengd,gridcell,depth.original=dypi,catch=afli,total,hooks,nr_net,
                num_traps,gear_size) %>%
  mar::encode_zchords(dx = 0.125,dy = 0.125/2) %>%
  dplyr::left_join(tbl_mar(mar,'ops$bthe."gr_tmp"')) %>%
  dplyr::mutate(r = depth.original/m) %>%
  dplyr::mutate(depth = ifelse(year %in% 2010:2012 & r > 1.5, depth.original, depth.original*1.8288)) %>%
  #dplyr::select(-dx, -dy, -x, -y, -area, -sq, -r) %>%
  dplyr::filter(year < 2022) %>%
  union_all(
    mar::adb_trip(mar) %>%
      mutate(year = year(departure),
             month = month(departure)) %>%
      filter(year > 2021) %>%
      rename(trip_registered = registered) %>%
      left_join(mar::adb_station(mar) %>%
                  mutate(towtime = round(24*60*(fishing_end - fishing_start)),## change to minutes
                         latitude = as.numeric(latitude),
                         longitude = as.numeric(longitude),
                         gridcell = d2sr(latitude, longitude)) %>%
                  rename(station_registered = registered,
                         station_note = note,
                         station_modified = modified), by = 'trip_id') %>%
      dplyr::left_join(tbl(mar,'gear_mapping'),by=c('gear_no'='veidarfaeri')) %>%
      left_join(mar::adb_catch(mar) %>%
                  left_join(tbl_mar(mar,'kvoti.studlar') %>%
                              filter(year(i_gildi) == 2019) %>%
                              select(species_no = ftegund, i_oslaegt),
                            by = 'species_no') %>%
                  filter(catch_type == 'CATC') %>%
                  rename(catch_note = note,
                         catch_modified = modified) %>%
                  mutate(quantity = quantity * nvl(i_oslaegt,1)),
                by = 'station_id') %>%
      left_join(mar::adb_catch(mar) %>%
                  select(-length, -sex_no) %>%
                  filter(catch_type == 'CATC') %>%
                  group_by(station_id) %>%
                  summarise(total = sum(quantity)),
                by = 'station_id') %>%
      left_join(mar::adb_line_net(mar),
                by = 'station_id') %>%
      left_join(mar::adb_dredge(mar) %>%
                  rename(dredge_width = width,
                         dredge_length = length,
                         dredge_height = height) %>%
                  select(-weight,-mesh_size,-mesh_type),
                by = 'station_id') %>%
      left_join(mar::adb_trap(mar), by = 'station_id') %>%
      mutate(gear_size = -1) |>
      dplyr::select(id=station_id,species = species_no,towtime,gear,vessel_nr=vessel_no,year,month,
                    lat=latitude,lon = longitude, gridcell,depth=depth,catch=quantity,total,hooks,nr_net = nets,
                    num_traps = number_of_traps, gear_size) %>%
      mar::encode_zchords(dx = 0.125,dy = 0.125/2)
  ) %>%
  dplyr::compute(name='logbooks_compiled',temporary=FALSE)
dbExecute(mar, 'grant select on "logbooks_compiled" to public')


#
# left_join(mar::adb_trawl_seine_net(mar),
#           by = 'station_id')



