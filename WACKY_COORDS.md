# Wacky coordinate error — reference notes

> This document is extracted from `AGENTS.md`. The wacky coordinate issue is a
> side-topic relative to the main logbook merge work; full methodology is in
> `wacky_recovery.qmd`.

## Background

A systematic encoding error affects `lengd` and `breidd` in
`rafr_sjalfvirkir_maelar.parquet` (and potentially other tables that passed
through the same conversion). Positions were stored as integers in **DDMMmm**
format (degrees–minutes–decimal-minutes, last two digits = 1/100th of a minute,
range 00–99) but were incorrectly converted to decimal degrees by treating the
last two digits as *seconds* (valid range 00–59). This misplaces reported
positions by up to ~0.4 NM when the decimal-minute value was ≥ 60.

## Confidence classification

Every affected record can be assigned one of three recovery levels:

| Level | Criterion | Positional uncertainty | Typical share |
|---|---|---|---|
| **High** | Extracted `ss` ∈ [40, 59] for **both** coords | Exact | ~4% |
| **Partial** | Extracted `ss` ∈ [40, 59] for **one** coord | ~300–740 m in one direction | ~33% |
| **Low** | Extracted `ss` ∈ [0, 39]  for **both** coords | ~300–740 m in both directions | ~63% |

## Recovery functions

All utility functions are defined in `wacky_recovery.qmd` (Functions section).
Key entry points:

```r
# Add recovery columns and confidence classification
dat <- add_recovery_cols(dat)   # adds ss_lon, ss_lat, confidence, lon_A/B, lat_A/B

# Resolve ambiguous records (greedy forward pass)
dat |>
  arrange(visir, timi) |>
  group_by(visir) |>
  group_modify(~ resolve_track(.x)) |>    # adds lon_r, lat_r
  ungroup()

# Resolve with forward-backward smoother (preferred for long tracks)
dat |>
  arrange(visir, timi) |>
  group_by(visir) |>
  group_modify(~ resolve_track_fb(.x)) |>  # adds lon_fb, lat_fb
  ungroup()
```

The smoother uses `skip_hradi` and `skip_stefna` (speed and heading) to
dead-reckon the expected next position and select between candidate A and B.
Median turn-angle improvement: **33–48%** by gear type (independent validation).

## DuckDB equivalent

A SQL macro for the *correct* conversion is registered in
`scripts/01_logbooks-old_convert.R`:

```sql
CREATE OR REPLACE MACRO rb_convert_DMdM(x) AS (
  SIGN(x) * (ABS(x) + (200.0/3.0) *
    ((ABS(x)/100.0) - TRUNC(ABS(x)/10000.0) * 100.0)) / 10000.0
);
```

## Outstanding work

- [ ] **Apply recovery** to the full `rafr_sjalfvirkir_maelar` dataset
  (2008–2020); write corrected parquet to `data/`; notify `../fishydata`.
- [ ] **Performance: `resolve_track_fb()`** — inner loop is pure R; consider
  Rcpp or `furrr::future_map()` for the full ~20M record dataset.
- [ ] **Clarify `logbook/` coordinate exposure** — determine which coordinate
  columns in the experimental `logbook/` schema (if any) are affected by the
  same wacky conversion. The `fs_afladagbok` DMS/DDM ambiguity is a separate
  issue already handled in the convert script.
