#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  if (!requireNamespace("optparse", quietly = TRUE)) {
    stop("Package 'optparse' is required. Install it before running this script.")
  }
  if (!requireNamespace("fixest", quietly = TRUE)) {
    stop("Package 'fixest' is required. Install it before running this script.")
  }
  library(optparse)
  library(fixest)
})

PERIOD_ORDER <- c("2000_2007", "2008_2012", "2000_2015", "2005_2010", "2005_2012")
TRANSFORM_ORDER <- c(
  "level_t", "diff_t", "difflag_t",
  "log_t", "logdiff_t", "logdifflag_t",
  "level_xtlag", "diff_xtlag", "difflag_xtlag",
  "log_xtlag", "logdiff_xtlag", "logdifflag_xtlag"
)
CONTROL_CANDIDATES <- c("urban_share", "share_hs_or_above", "literacy_rate")
get_cluster_var <- function() {
  config_path <- file.path("config", "params.yaml")
  cluster <- NA_character_
  if (file.exists(config_path)) {
    lines <- readLines(config_path, warn = FALSE)
    pattern <- "^\\s*cluster\\s*:\\s*(.+)\\s*$"
    match_idx <- grep(pattern, lines)
    if (length(match_idx) > 0) {
      cluster <- sub(pattern, "\\1", lines[match_idx[1]])
    }
  }
  cluster <- trimws(cluster)
  if (length(cluster) == 0 || is.na(cluster)) {
    "district_bps_2000"
  } else {
    cluster
  }
}

option_list <- list(
  make_option(c("-m", "--mode"), default = "dummy",
              help = "Execution mode (dummy or real). [default %default]"),
  make_option(c("-o", "--outcome"), default = "p0",
              help = "Outcome column to use (p0, p1, p2, ...)."),
  make_option(c("-f", "--flow"), help = "Flow code (import/export)."),
  make_option(c("-p", "--partner"), help = "Partner ISO code."),
  make_option(c("-n", "--num"), type = "integer", default = 6,
              help = "Maximum number of column specs to include (default: 6).")
)

opt <- parse_args(OptionParser(option_list = option_list))

if (is.null(opt$flow) || is.null(opt$partner)) {
  stop("Flow and partner must be specified.")
}

mode <- opt$mode
outcome <- opt$outcome
flow <- opt$flow
partner <- opt$partner
max_cols <- opt$num

panel_path <- file.path("data_work", "processed", mode, "panel_analysis.csv")
if (!file.exists(panel_path)) {
  stop(sprintf("Panel data not found at %s", panel_path))
}

panel <- read.csv(panel_path)
panel <- panel[panel$flow_code == flow & panel$partner_iso3 == partner, ]
if (!outcome %in% names(panel)) {
  stop(sprintf("Outcome '%s' not found in panel data.", outcome))
}

cluster_var <- get_cluster_var()
if (!cluster_var %in% names(panel)) {
  warning(sprintf("Cluster column '%s' not found in panel; falling back to 'district_bps_2000'.", cluster_var))
  cluster_var <- "district_bps_2000"
}
cluster_formula <- as.formula(paste0("~", cluster_var))

results_path <- file.path("data_work", "output", mode, "results", sprintf("main_%s.csv", outcome))
if (!file.exists(results_path)) {
  stop(sprintf("Results file '%s' not found. Run the pipeline first.", results_path))
}

results <- read.csv(results_path, stringsAsFactors = FALSE)
results <- results[
  results$flow == flow &
    results$partner == partner &
    results$outcome == outcome,
  ,
  drop = FALSE
]

if (nrow(results) == 0) {
  stop("No matching specifications found in results for the provided flow/partner/outcome.")
}

results$transform_rank <- match(results$transform_id, TRANSFORM_ORDER, nomatch = length(TRANSFORM_ORDER) + 1L)
results$period_rank <- match(results$period, PERIOD_ORDER, nomatch = length(PERIOD_ORDER) + 1L)
results <- results[order(results$transform_rank, results$iv_label, results$base_year, results$period_rank), ]

spec_cols <- c(
  "transform_id", "transform_label", "exposure_var", "instrument_var",
  "outcome_var", "base_year", "period", "iv_label"
)
spec_grid <- unique(results[, spec_cols])

period_cols <- grep("^period_", names(panel), value = TRUE)
if (length(period_cols) == 0) {
  stop("No period indicator columns found (expected columns beginning with 'period_').")
}

model_list <- list()
count <- 0L

for (idx in seq_len(nrow(spec_grid))) {
  spec <- spec_grid[idx, , drop = FALSE]
  period_col <- paste0("period_", spec$period)
  if (!period_col %in% names(panel)) {
    next
  }

  exposure_var <- spec$exposure_var
  instrument_var <- spec$instrument_var
  outcome_var <- spec$outcome_var

  required_cols <- c(outcome_var, exposure_var, instrument_var, period_col, "base_year", "iv_label")
  missing_cols <- setdiff(required_cols, names(panel))
  if (length(missing_cols) > 0) {
    warning(sprintf("Skipping spec due to missing columns: %s", paste(missing_cols, collapse = ", ")))
    next
  }

  subset <- panel[
    panel$base_year == spec$base_year &
      panel[[period_col]] == 1 &
      panel$iv_label == spec$iv_label,
    ,
    drop = FALSE
  ]

  available_controls <- intersect(CONTROL_CANDIDATES, names(subset))
  complete_cols <- c(outcome_var, exposure_var, instrument_var, available_controls)
  subset <- subset[complete.cases(subset[, complete_cols, drop = FALSE]), ]

  if (nrow(subset) < 100) {
    next
  }

  subset$district_bps_2000 <- factor(subset$district_bps_2000)
  subset$year <- factor(subset$year)
  if (cluster_var %in% names(subset)) {
    subset[[cluster_var]] <- factor(subset[[cluster_var]])
  }

  rhs_terms <- c(exposure_var, available_controls)
  rhs_str <- paste(rhs_terms, collapse = " + ")
  fe_str <- "district_bps_2000 + year"
  iv_formula <- as.formula(sprintf(
    "%s ~ %s | %s | (%s ~ %s)",
    outcome_var,
    rhs_str,
    fe_str,
    exposure_var,
    instrument_var
  ))

  model <- tryCatch(
    feols(iv_formula, data = subset, cluster = cluster_formula),
    error = function(e) {
      warning(sprintf(
        "Failed to estimate spec (transform %s, base %s, period %s, IV %s): %s",
        spec$transform_id,
        spec$base_year,
        spec$period,
        spec$iv_label,
        e$message
      ))
      NULL
    }
  )
  if (is.null(model)) {
    next
  }

  label <- sprintf(
    "%s | Base %s, %s | IV %s",
    spec$transform_label,
    spec$base_year,
    gsub("_", "–", spec$period),
    spec$iv_label
  )

  model_list[[label]] <- model
  count <- count + 1L
  if (count >= max_cols) {
    break
  }
}

if (!length(model_list)) {
  warning(sprintf("No models estimated for flow %s / partner %s", flow, partner))
  quit(status = 0)
}

output_dir <- file.path("data_work", "output", mode, "tables")
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

model_args <- unname(model_list)
names(model_args) <- NULL

# HTML export
etable_args <- c(model_args,
                 list(se = "cluster",
                      cluster = cluster_formula,
                      signif.code = c("***" = 0.01, "**" = 0.05, "*" = 0.1),
                      digits = 3,
                      export = file.path(output_dir, sprintf("%s_%s_%s_etable.html", outcome, flow, partner))))
do.call(etable, etable_args)

# LaTeX export
etable_args$export <- file.path(output_dir, sprintf("%s_%s_%s_etable.tex", outcome, flow, partner))
etable_args$tex <- TRUE
do.call(etable, etable_args)
