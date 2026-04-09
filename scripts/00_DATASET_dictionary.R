dictionary <-
  tibble::tribble(~new, ~name,
                  ".sid", "visir",
                  "vid", "skipnr",
                  "gid", "veidarf",
                  "n_crew", "ahofn",
                  "year", "ar",
                  "month", "veman",
                  "date", "vedags",
                  "x1", "lengd",
                  "y1", "breidd",
                  "x2", "lengd_lok",
                  "y2", "breidd_lok",
                  "sq", "reitur",
                  "ssq", "smareitur",
                  "z1", "dypi",
                  "z2", "dypi_lok",
                  "winddirection", "vindatt",
                  "beaufort", "vindstig",
                  "m_sec", "m_sek",
                  "distance", "toglengd",   # Derived measure
                  "D2", "ldags",            # Landing date
                  "hid2", "lhofn",          # Harbour id landings took place
                  "sid", "tegund",
                  "catch", "afli",
                  "towtime", "togtimi",
                  "on.bottom", "ibotni",
                  "mesh", "moskvi",
                  "mesh_min", "moskvi_minnsti",
                  "doors", "hlerar",              # in kilograms
                  "headline", "hoflina",
                  "sweeps", "grandarar",          # in meters ???
                  "plow_width", "pl_breidd",
                  "tempb1", "botnhiti",           # bottom temperature
                  "tempb2", "botnhiti_lok",
                  "temps1",  "uppsj_hiti",         # surface temperature
                  "temps2", "uppsj_hiti_lok",
                  "height", "haed",           # gillnets
                  "mean_gillnet_length", "medal_lengd_neta",
                  "bait", "beita",
                  "temps1", "uppsjavarhiti",  # surface temperature
                  "t0", "logn_hefst",         # time setting starts
                  "t1", "drattur_hefst",      # time gear hauling starts
                  "t2", "drattur_lykur",      # time gear hauling ends
                  "n_units", "gildrur",
                  "hours", "klst",
                  "vid", "vessel_no",
                  "D1", "departure",
                  "hid1", "departure_port_no",
                  "D2", "landing",
                  "hid2", "landing_port_no",
                  ".tid", "trip_id",
                  ".sid", "station_id",
                  "gid", "gear_no",
                  "t1", "fishing_start",
                  "t2", "fishing_end",
                  "x1", "longitude",
                  "y1", "latitude",
                  "x2", "longitude_end",
                  "y2", "latitude_end",
                  "z1", "depth",
                  "z2", "depth_end",
                  "t1", "tow_start",
                  "vessel", "skip_nafn",
                  "cs", "kallmerki",
                  "uid", "umdaemisnumer",
                  "D1", "upphafstimi",
                  "hid1", "upphafshofn",
                  "D2", "londunardagur",
                  "hid2", "londunarhofn",
                  ".tid", "veidiferd_id",
                  "gid", "veidarfaeri_id",
                  "t1", "upphaf_timi",
                  "t2", "lok_timi",
                  "y1", "upphaf_breidd",
                  "x1", "upphaf_lengd",
                  "y2", "lok_breidd",
                  "x2", "lok_lengd",
                  "z1", "upphaf_dypi",
                  "z2", "lok_dypi",
                  "t12", "milli_timi",
                  "n_lost", "fj_tapadra_veidarfaera"
  )
dictionary |> duckdbfs::write_dataset("data/dictionary.parquet")
