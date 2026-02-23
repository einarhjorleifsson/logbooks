#' Translate column names of a data.frame or lazy tibble using a dictionary
#'
#' This function allows renaming column names in an input object (a `data.frame` or `tbl_lazy`) 
#' by translating them with a user-supplied dictionary. Users can choose to translate 
#' column names from "old" to "new" or vice versa.
#'
#' @param d A `data.frame` or `tbl_lazy` object whose column names need to be translated.
#' @param dictionary A `data.frame` (or tibble) with at least two columns: `from` and `to` 
#'   (default names are "old" and "new"). The `from` column contains the current column names 
#'   in `d`, and the `to` column contains the new column names to translate to.
#' @param from A string specifying the column name in `dictionary` to use for current column 
#'   name matching. Defaults to "old".
#' @param to A string specifying the column name in `dictionary` with new column names 
#'   to translate to. Defaults to "new".
#'
#' @return An object of the same class as `d` with column names translated.
#'
#' @examples
#' # Example dictionary
#' dictionary <- tibble::tibble(
#'   recordtype = c("HH", "HH"),
#'   new = c("Survey_Name", "Year_New"),
#'   old = c("Survey", "Year")
#' )
#'
#' # Example data.frame
#' df <- data.frame(Survey = "NS-IBTS", Year = 2025, Quarter = 1)
#' translate_names(df, dictionary)
#'
#' # Example lazy tibble connection
#' con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
#' copy_to(con, mtcars, "mtcars")
#' lazy_tbl <- dplyr::tbl(con, "mtcars")
#' translate_names(lazy_tbl, tibble::tibble(old = "hp", new = "horsepower"))
#'
#' @export
translate_names <- function(d, dictionary, from = "old", to = "new") {
  # Input checks
  if (!inherits(d, c("data.frame", "tbl_lazy"))) {
    stop("`d` must be either a data.frame or a tbl_lazy object.")
  }
  
  if (!is.data.frame(dictionary)) {
    stop("`dictionary` must be a data.frame or tibble.")
  }
  
  if (!all(c(from, to) %in% colnames(dictionary))) {
    stop("`dictionary` must contain the specified `from` and `to` columns.")
  }
  
  # Perform the translation
  d <- dplyr::rename_with(d, .fn = function(col) {
    # Match column names in the 'from' column and translate to 'to' column
    new_names <- dictionary[[to]][match(col, dictionary[[from]])]
    # Keep original column names if there is no match
    ifelse(is.na(new_names), col, new_names)
  })
  return(d)
}

