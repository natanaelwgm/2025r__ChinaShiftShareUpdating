run_validations <- function(labor_info, exposures, instruments, estimation_results, params) {
  base_years <- params$analysis$base_years
  share_list <- labor_info$shares
  share_metrics <- list()
  for (base_year in base_years) {
    share_matrix <- NULL
    key <- as.character(base_year)
    if (!is.null(share_list)) {
      share_matrix <- share_list[[key]]
    }
    if (is.null(share_matrix)) {
      share_matrix <- labor_info[[paste0("shares_", key)]]
    }
    if (!is.null(share_matrix)) {
      totals <- rowSums(share_matrix, na.rm = TRUE)
      share_metrics[[paste0("share", key, "_mean")]] <- mean(totals, na.rm = TRUE)
      share_metrics[[paste0("share", key, "_min")]] <- min(totals, na.rm = TRUE)
      share_metrics[[paste0("share", key, "_max")]] <- max(totals, na.rm = TRUE)
    }
  }

  exposure_var <- stats::var(exposures$exposure_log_diff, na.rm = TRUE)
  instrument_non_missing <- mean(!is.na(instruments$instrument_log_diff_lag1))

  c(
    share_metrics,
    list(
      exposure_variance = exposure_var,
      instrument_non_missing_ratio = instrument_non_missing,
      regression_rows = nrow(estimation_results$main),
      first_stage_rows = nrow(estimation_results$first_stage)
    )
  )
}

write_validation_report <- function(report, path) {
  if (!requireNamespace("jsonlite", quietly = TRUE)) {
    warning("Package 'jsonlite' not available; writing validation report as plain text.")
    dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
    capture.output(str(report), file = path)
    return(invisible(NULL))
  }
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  jsonlite::write_json(report, path, pretty = TRUE, auto_unbox = TRUE)
}
