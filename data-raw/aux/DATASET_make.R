library(sf)
library(tidyverse)
sf_use_s2(FALSE)


# FAO_AREAS_ERASE --------------------------------------------------------------
fao <- read_sf("data-dump/aux/FAO_AREAS_ERASE.geojson") |> select(-1)
levels <- c("SUBUNIT", "SUBDIVISION", "DIVISION", "SUBAREA", "MAJOR")

fao <- st_make_valid(fao)

erase_covered <- function(geom, covered) {
  tryCatch(
    st_difference(st_sfc(geom, crs = st_crs(covered)), covered)[[1]],
    error = function(e) st_geometrycollection()
  )
}

result <- fao |> filter(F_LEVEL == levels[1])
covered <- st_union(result)

for (lvl in levels[-1]) {
  candidates <- fao |> filter(F_LEVEL == lvl)
  new_geoms  <- lapply(st_geometry(candidates), erase_covered, covered = covered)
  st_geometry(candidates) <- st_sfc(new_geoms, crs = st_crs(candidates))
  new_parts  <- candidates |> filter(!st_is_empty(geometry))
  result  <- bind_rows(result, new_parts)
  covered <- st_union(result)
}

result |> write_sf("data-raw/aux/FAO_AREAS_ERASE.gpkg")

# FAO_AREAS_NOCOASTLINE --------------------------------------------------------
fao <- read_sf("data-dump/aux/FAO_AREAS_NOCOASTLINE.geojson") |> select(-1)
fao <- st_make_valid(fao)

erase_covered <- function(geom, covered) {
  tryCatch(
    st_difference(st_sfc(geom, crs = st_crs(covered)), covered)[[1]],
    error = function(e) st_geometrycollection()
  )
}

result <- fao |> filter(F_LEVEL == levels[1])
covered <- st_union(result)

for (lvl in levels[-1]) {
  candidates <- fao |> filter(F_LEVEL == lvl)
  new_geoms  <- lapply(st_geometry(candidates), erase_covered, covered = covered)
  st_geometry(candidates) <- st_sfc(new_geoms, crs = st_crs(candidates))
  new_parts  <- candidates |> filter(!st_is_empty(geometry))
  result  <- bind_rows(result, new_parts)
  covered <- st_union(result)
}
result |> write_sf("data-raw/aux/FAO_AREAS_NOCOASTLINE.gpkg")
