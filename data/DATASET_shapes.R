# Build H3 lookup table for ICES VMS datacall spatial joins --------------------
#
# Input:
#   data-raw/aux/ices_areas.fgb   — 66 ICES statistical areas
#   data-raw/aux/ICES_GEBCO.rds   — 9 ICES/GEBCO depth classes (column: depth)
#   data-raw/aux/eusm.rds         — 2.18M EmodNet seabed habitat polygons
#                                   (27 MSFD_BBHT classes, WGS 84)
#
# Output:
#   data/aux/h3_lookup.parquet    — one row per H3 res-9 cell; columns:
#                                   h3, ices_area, depth_class, MSFD_BBHT
#
# All three objects are rasterized to the same 0.005° template covering the
# ICES domain, then H3 resolution-9 cell IDs are assigned from each raster
# cell centroid. Consistent approach across all three objects.
#
# DuckDB join:
#   LEFT JOIN read_parquet('data/aux/h3_lookup.parquet') h
#     ON geo_to_h3(t.lat, t.lon, 9) = h.h3

library(sf)
library(terra)
library(tidyverse)
library(h3)

zoom <- 9L

# Load spatial objects ---------------------------------------------------------
# there still the issue of a buffer inland from the shoreline
ices_areas <- read_sf("data-raw/aux/ices_areas.fgb") |> select(fao = Area_Full)
# ... rest downstream

# Raster template: ICES domain extent at 0.005° (~500m resolution)
r <- rast(ext(vect(ices_areas)), resolution = 0.005, crs = "EPSG:4326")

# Helper -----------------------------------------------------------------------
# Encode a character column as integer, rasterize to template, extract H3 lookup.
# Returns a two-column tibble: h3, <label_col>.
rast_to_h3 <- function(sf_obj, label_col, raster_template, res) {
  levels_tbl <- tibble(label = sort(unique(sf_obj[[label_col]]))) |>
    mutate(id = row_number())
  sf_obj <- sf_obj |>
    mutate(id = levels_tbl$id[match(.data[[label_col]], levels_tbl$label)])
  r_out <- rasterize(vect(sf_obj), raster_template, field = "id", fun = "min") |>
    trim()
  as.data.frame(r_out, xy = TRUE, na.rm = TRUE) |>
    as_tibble() |>
    rename(lon = x, lat = y, id = 3) |>
    left_join(levels_tbl, by = "id") |>
    mutate(h3 = geo_to_h3(data.frame(lat, lon), res = res)) |>
    count(h3, label) |>
    slice_max(n, n = 1, with_ties = FALSE, by = h3) |>
    select(h3, label) |>
    rename(!!label_col := label)
}

# ICES statistical areas -------------------------------------------------------
ices_areas_h3 <- rast_to_h3(ices_areas, "fao", r, zoom)

# ICES/GEBCO depth classes -----------------------------------------------------
ices_gebco <- readRDS("data-raw/aux/ICES_GEBCO.rds") |> select(depth_class = depth)
gebco_h3 <- rast_to_h3(ices_gebco, "depth_class", r, zoom)

# eusm seabed habitats ---------------------------------------------------------
sf::sf_use_s2(FALSE)
eusm <- readRDS("data-raw/aux/eusm.rds") |>
  rename(msfd_bbht = MSFD_BBHT,
         geometry = Shape) |>
  # a bit daft to include these
  filter(msfd_bbht != "") |>
  st_make_valid() |>
  st_make_valid()

eusm <- eusm[st_is_valid(eusm$geometry), ] |>
  st_crop(st_bbox(ices_areas)) |>
  st_cast("MULTIPOLYGON")

eusm_h3 <- rast_to_h3(eusm, "msfd_bbht", r, zoom)

# Combine and write ------------------------------------------------------------
h3_lookup <-
  ices_areas_h3 |>
  full_join(gebco_h3, by = "h3") |>
  full_join(eusm_h3,  by = "h3")

duckdbfs::write_dataset(h3_lookup, "data/aux/h3_lookup.parquet")

