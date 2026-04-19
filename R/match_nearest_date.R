#' Match logbook records to the nearest date in a landing table
#'
#' For each unique `(vid, datel)` pair in `lb`, finds the row in `ln` with the
#' same `vid` and the closest `datel`, then joins back `gid_ln` and `.lid` from
#' that matched row.
#'
#' @param lb A data frame containing at minimum columns `vid` and `datel`.
#' @param ln A data frame containing at minimum columns `vid`, `datel`,
#'   `gid_ln`, and `.lid`.
#' @param method Character string specifying the matching backend. Either
#'   `"data.table"` (default) or `"dplyr"`. Both return the same result;
#'   `"data.table"` breaks ties by taking the later date, `"dplyr"` breaks
#'   ties arbitrarily.
#'
#' @return A data frame with the same rows as `lb`, with columns `date.ln`,
#'   `gid_ln`, and `.lid` appended.
#'
#' @examples
#' \dontrun{
#' lb_matched <- xmatch_nearest_date(lb, ln, method = "dplyr")
#' }
match_nearest_date <- function(lb, ln, method = "data.table") {

  method <- match.arg(method, choices = c("data.table", "dplyr"))

  if (method == "data.table") {

    lb.dt <-
      lb |>
      select(vid, datel) |>
      distinct() |>
      data.table::setDT()

    ln.dt <-
      ln |>
      select(vid, datel) |>
      distinct() |>
      mutate(dummy = datel) |>
      data.table::setDT()

    nearest <-
      lb.dt[, date.ln := ln.dt[lb.dt, dummy, on = c("vid", "datel"), roll = "nearest"]] |>
      tibble::as_tibble()

  } else {

    nearest <-
      lb |>
      select(vid, datel) |>
      distinct() |>
      left_join(
        ln |> select(vid, datel) |> distinct() |> rename(date.ln = datel),
        join_by(vid),
        relationship = "many-to-many"
      ) |>
      mutate(diff = abs(as.numeric(datel - date.ln))) |>
      slice_min(diff, by = c(vid, datel), n = 1, with_ties = FALSE) |>
      select(vid, datel, date.ln)

  }

  lb |>
    left_join(nearest, by = c("vid", "datel")) |>
    left_join(ln |> select(vid, date.ln = datel, gid_ln, .lid),
              by = c("vid", "date.ln"))

}
