run_validations <- function(labor_info, exposures, instruments, estimation_results) {
  share2000 <- rowSums(labor_info$shares_2000)
  share2008 <- rowSums(labor_info$shares_2008)

  exposure_var <- stats::var(exposures$exposure_log_diff, na.rm = TRUE)
  instrument_non_missing <- mean(!is.na(instruments$instrument_log_diff_lag1))

  list(
    share2000_mean = mean(share2000),
    share2008_mean = mean(share2008),
    share2000_min = min(share2000),
    share2000_max = max(share2000),
    share2008_min = min(share2008),
    share2008_max = max(share2008),
    exposure_variance = exposure_var,
    instrument_non_missing_ratio = instrument_non_missing,
    regression_rows = nrow(estimation_results$main),
    first_stage_rows = nrow(estimation_results$first_stage)
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

