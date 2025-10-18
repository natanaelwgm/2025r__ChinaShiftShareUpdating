log_info <- function(...) {
  message(sprintf("[Orchestrator] %s", paste(..., collapse = " ")))
}

ensure_packages <- function(pkgs) {
  missing <- pkgs[!vapply(pkgs, requireNamespace, logical(1), quietly = TRUE)]
  if (length(missing) > 0) {
    stop(sprintf(
      "Missing required packages: %s. Please install them before running the pipeline.",
      paste(missing, collapse = ", ")
    ))
  }
}

ensure_directories <- function(paths) {
  for (path in paths) {
    if (!dir.exists(path)) {
      dir.create(path, recursive = TRUE, showWarnings = FALSE)
    }
  }
}

reset_directory <- function(path) {
  if (dir.exists(path)) {
    targets <- list.files(path, full.names = TRUE, all.files = TRUE, no.. = TRUE)
    if (length(targets) > 0) {
      unlink(targets, recursive = TRUE, force = TRUE)
    }
  }
  dir.create(path, recursive = TRUE, showWarnings = FALSE)
}

write_csv_safe <- function(df, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  utils::write.csv(df, path, row.names = FALSE)
  invisible(path)
}

write_partitioned_csv <- function(df, output_dir, prefix, split_cols = "year") {
  if (!all(split_cols %in% names(df))) {
    stop("All split columns must exist in data frame for partitioned write.")
  }
  reset_directory(output_dir)
  split_list <- split(df, df[split_cols], drop = TRUE)
  written_paths <- character(length(split_list))
  idx <- 1L
  for (key in names(split_list)) {
    key_df <- split_list[[key]]
    key_values <- lapply(split_cols, function(col) as.character(key_df[[col]][1]))
    key_label <- paste(paste0(split_cols, "_", unlist(key_values)), collapse = "_")
    filename <- sprintf("%s_%s.csv", prefix, key_label)
    out_path <- file.path(output_dir, filename)
    utils::write.csv(key_df, out_path, row.names = FALSE)
    written_paths[idx] <- out_path
    idx <- idx + 1L
  }
  invisible(written_paths)
}

read_partitioned_csv <- function(dir_path, pattern = "\\.csv$") {
  if (!dir.exists(dir_path)) {
    stop(sprintf("Directory '%s' not found.", dir_path))
  }
  files <- list.files(dir_path, pattern = pattern, full.names = TRUE)
  if (length(files) == 0) {
    stop(sprintf("No CSV files found in '%s'.", dir_path))
  }
  dfs <- lapply(files, utils::read.csv, stringsAsFactors = FALSE)
  do.call(rbind, dfs)
}

ensure_non_empty_dir <- function(dir_path, label) {
  if (!dir.exists(dir_path)) {
    stop(sprintf("Expected %s directory at '%s', but it does not exist.", label, dir_path))
  }
  files <- list.files(dir_path, pattern = "\\.csv$", full.names = TRUE)
  if (length(files) == 0) {
    stop(sprintf("Expected CSV files for %s in '%s', but none were found.", label, dir_path))
  }
  invisible(files)
}

artifact_path <- function(globals, category, filename) {
  base <- globals$paths[[category]]
  if (is.null(base)) {
    stop(sprintf("Unknown path category '%s' in globals config.", category))
  }
  file.path(base, filename)
}

period_to_bounds <- function(period_label) {
  parts <- strsplit(period_label, "_")[[1]]
  if (length(parts) != 2) {
    stop(sprintf("Invalid period label: %s", period_label))
  }
  start_year <- as.integer(parts[1])
  end_year <- as.integer(parts[2])
  c(start_year, end_year)
}

apply_period_flag <- function(years, period_label) {
  bounds <- period_to_bounds(period_label)
  years >= bounds[1] & years <= bounds[2]
}

lag_vector <- function(x, n = 1L) {
  if (n < 0L) stop("n must be non-negative")
  if (n == 0L) return(x)
  if (length(x) <= n) {
    return(rep(NA, length(x)))
  }
  c(rep(NA, n), x[seq_len(length(x) - n)])
}

group_lag <- function(x, groups, n = 1L) {
  if (n < 0L) stop("n must be non-negative")
  if (!is.list(groups)) {
    groups <- list(groups)
  }
  ave(x, groups, FUN = function(values) lag_vector(values, n))
}

group_diff <- function(x, groups) {
  if (!is.list(groups)) {
    groups <- list(groups)
  }
  ave(x, groups, FUN = function(values) {
    if (length(values) <= 1L) {
      rep(NA_real_, length(values))
    } else {
      c(NA, diff(values))
    }
  })
}

format_duration <- function(seconds) {
  if (seconds < 60) {
    return(sprintf("%.2f sec", seconds))
  }
  minutes <- floor(seconds / 60)
  rem <- seconds - minutes * 60
  if (minutes < 60) {
    return(sprintf("%dm %.1fs", minutes, rem))
  }
  hours <- floor(minutes / 60)
  minutes <- minutes - hours * 60
  sprintf("%dh %dm %.1fs", hours, minutes, rem)
}

with_step <- function(label, expr) {
  log_info(sprintf("▶ %s ...", label))
  start_time <- Sys.time()
  on.exit({
    elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
    log_info(sprintf("✔ %s (%s)", label, format_duration(elapsed)))
  }, add = TRUE)
  eval.parent(substitute(expr))
}

log_output_path <- function(label, path) {
  if (!is.null(path)) {
    log_info(sprintf("📁 %s saved at: %s", label, normalizePath(path, mustWork = FALSE)))
  }
}
