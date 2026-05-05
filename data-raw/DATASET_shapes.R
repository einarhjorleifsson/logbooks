library(sf)
library(tidyverse)
sf::sf_use_s2(FALSE)
# ICES areas -------------------------------------------------------------------
ices_areas <-
  read_sf("data-dump/aux/ICESareas.gpkg") |>
  select(fao = Area_Full) |>
  st_as_sf() |>
  st_make_valid() |>
  mutate(geom = lwgeom::lwgeom_make_valid(geom)) |>
  st_make_valid()
ices_areas$geom |> st_is_valid() |> table()
ices_areas |>
  write_sf("data-raw/aux/ices_areas.gpkg")
# gebco ------------------------------------------------------------------------
gebco <-
  readRDS("data-dump/aux/ICES_GEBCO.rds") |>
  rename(depth_range = depth, geom = geometry) |>
  st_as_sf() |>
  st_make_valid() |>
  mutate(geom = lwgeom::lwgeom_make_valid(geom)) |>
  st_make_valid()
gebco$geom |> st_is_valid() |> table()
gebco |>
  write_sf("data-raw/aux/gebco.gpkg")
# eusm -------------------------------------------------------------------------
eusm <-
  readRDS("data-dump/aux/eusm.rds") |>
  rename(bbht = MSFD_BBHT, geom = Shape) |>
  # drops 986652 features !!
  filter(bbht != "") |>
  st_as_sf() |>
  st_make_valid() |>
  mutate(geom = lwgeom::lwgeom_make_valid(geom)) |>
  st_make_valid()
eusm$geom |> st_is_valid() |> table()
eusm |>
  write_sf("data-raw/aux/eusm.gpkg")
