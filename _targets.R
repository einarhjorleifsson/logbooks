# _targets.R — pipeline entry point for the {targets} package
#
# {targets} tracks every input and output of your pipeline as a "target".
# When you call tar_make(), it only re-runs targets whose inputs have changed
# since the last run — everything else is loaded from cache.
#
# Key commands (run these in the R console, not here):
#   targets::tar_make()              — run the whole pipeline (skips up-to-date targets)
#   targets::tar_make("target_name") — run only that target + its dependencies
#   targets::tar_visnetwork()        — interactive dependency graph in the Viewer pane
#   targets::tar_read("target_name") — retrieve a completed target's value
#   targets::tar_outdated()          — list targets that would re-run if you called tar_make()
#   targets::tar_destroy()           — wipe the cache (_targets/ folder) and start fresh
#
# Dependencies between targets are declared implicitly: if you use one target's
# *name* inside another target's command, {targets} knows to run it first and
# will re-run the downstream target whenever the upstream result changes.

library(targets)

# Load helper functions from R/ ------------------------------------------------
# tar_source() is like source() but targets also watches these files for changes.
tar_source("R/")

# Global options ---------------------------------------------------------------
# Packages listed here are pre-loaded in every target's R sub-process, so you
# don't need to call library() inside each target command.
tar_option_set(
  packages = c("tidyverse", "nanoparquet", "whack", "geo")
)

# Pipeline definition ----------------------------------------------------------
# The pipeline is a plain R list of tar_target() calls.
#
# tar_target() anatomy:
#   name    — the symbol you use to refer to this target elsewhere in the pipeline
#   command — an R expression that produces the target's value
#   format  — how targets stores the value; "file" means the command returns a
#             file *path* and targets hashes the file's content (not just the path)
#             so any downstream target re-runs when the file actually changes.
#
# format = "file" is the right choice whenever a target reads or writes disk
# files (parquet, CSV, etc.).  The command must return a character vector of
# file paths.

list(

  # ── Stage 0a: Track input scripts as file targets ───────────────────────────
  # By declaring each script as a file target, targets will detect when you edit
  # the script and automatically mark all downstream targets as outdated.
  # The command here is just the file path string — targets hashes the file.

  tar_target(script_dict,  "data-raw/DATASET_dictionary.R",     format = "file"),
  tar_target(script_gear,  "data-raw/DATASET_gear-codes.R",      format = "file"),
  tar_target(script_afli,  "data-raw/logbooks/01_afli_convert.R",          format = "file"),
  tar_target(script_fs,    "data-raw/logbooks/01_fs_afladagbok_convert.R", format = "file"),
  tar_target(script_adb,            "data-raw/logbooks/01_adb_convert.R",          format = "file"),
  tar_target(script_merge,          "data/logbooks/logbooks.R",                             format = "file"),
  tar_target(script_landings,       "data/landings/landings.R",                             format = "file"),
  #tar_target(script_landings_match, "data/logbooks-landings_match.R",              format = "file"),


  # ── Stage 0b: Lookup tables ──────────────────────────────────────────────────
  # These hand-maintained scripts build the reference parquet files (dictionary
  # and gear mapping) used by all three convert scripts.
  # Referencing script_dict / script_gear in the command body declares the
  # dependency: if either script file changes, this target re-runs.

  tar_target(
    name    = dictionary_file,
    command = {
      script_dict                                  # re-run when script changes
      source("data-raw/DATASET_dictionary.R")
      "data/dictionary.parquet"                    # return the output file path
    },
    format = "file"
  ),

  tar_target(
    name    = gear_mapping_file,
    command = {
      script_gear
      source("data-raw/DATASET_gear-codes.R")
      "data/gear/gear_mapping.parquet"
    },
    format = "file"
  ),


  # ── Stage 1: Schema conversions ──────────────────────────────────────────────
  # Each target converts one raw schema to the unified trip / station / catch
  # format and returns the three output file paths.  These three targets have no
  # dependency on each other, so {targets} can run them in parallel when you use
  # tar_make(callr_function = callr::r_bg, workers = 3) or a crew controller.
  #
  # All three depend on dictionary_file and gear_mapping_file (Tier 0), so those
  # will always be built first.

  tar_target(
    name    = afli_files,
    command = {
      dictionary_file                              # must exist before this runs
      gear_mapping_file
      script_afli                                  # re-run when script changes
      source("data-raw/logbooks/01_afli_convert.R")
      c("data-raw/logbooks/afli/trip.parquet",
        "data-raw/logbooks/afli/station.parquet",
        "data-raw/logbooks/afli/catch.parquet")
    },
    format = "file"
  ),

  tar_target(
    name    = fs_files,
    command = {
      dictionary_file
      gear_mapping_file
      script_fs
      source("data-raw/logbooks/01_fs_afladagbok_convert.R")
      c("data-raw/logbooks/fs_afladagbok/trip.parquet",
        "data-raw/logbooks/fs_afladagbok/station.parquet",
        "data-raw/logbooks/fs_afladagbok/catch.parquet")
    },
    format = "file"
  ),

  tar_target(
    name    = adb_files,
    command = {
      dictionary_file
      gear_mapping_file
      script_adb
      source("data-raw/logbooks/01_adb_convert.R")
      c("data-raw/logbooks/adb/trip.parquet",
        "data-raw/logbooks/adb/station.parquet",
        "data-raw/logbooks/adb/catch.parquet")
    },
    format = "file"
  ),


  # ── Stage 2: Merge ───────────────────────────────────────────────────────────
  # Runs after all three conversions are complete.  Depends on afli_files,
  # fs_files, and adb_files — if any of those parquet files change (e.g. because
  # a convert script was fixed), the merge automatically re-runs.

  tar_target(
    name    = merged_files,
    command = {
      afli_files                                   # Stage 1 outputs must exist
      fs_files
      adb_files
      gear_mapping_file
      script_merge
      source("data/logbooks/logbooks.R")
      c("data/logbooks/trip.parquet",
        "data/logbooks/station.parquet",
        "data/logbooks/catch.parquet")
    },
    format = "file"
  ),


  # ── Stage 3: Landings ────────────────────────────────────────────────────────

  tar_target(
    name    = landings_files,
    command = {
      script_landings
      source("data/landings/landings.R")
      c("data/landings/landings.parquet",
        "data/landings/catch.parquet")
    },
    format = "file"
  )


  # ── Stage 4: Landings match ──────────────────────────────────────────────────
  # Builds a .tid → .lid crosswalk between merged logbook trips and the landing
  # register.  Depends on merged_files (trip/station/catch) being up to date.

  # tar_target(
  #   name    = landings_match_file,
  #   command = {
  #     merged_files                                 # trip/station/catch must exist
  #     script_landings_match
  #     source("data/logbooks-landings_match.R")
  #     "data/logbooks/lid_map.parquet"
  #   },
  #   format = "file"
  # )

)
