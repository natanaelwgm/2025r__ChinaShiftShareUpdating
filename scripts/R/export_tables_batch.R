#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  if (!requireNamespace("optparse", quietly = TRUE)) {
    stop("Package 'optparse' is required. Install it before running this script.")
  }
  library(optparse)
})

option_list <- list(
  make_option(c("-m", "--mode"), default = "dummy",
              help = "Execution mode (dummy or real). [default %default]"),
  make_option(c("-o", "--outcome"), default = "p0",
              help = "Outcome to export (p0, p1, p2). [default %default]"),
  make_option(c("-n", "--num"), type = "integer", default = 5,
              help = "Number of (flow, partner) combos to export. [default %default]")
)

opt <- parse_args(OptionParser(option_list = option_list))

mode <- tolower(opt$mode)
if (!mode %in% c("dummy", "real")) {
  stop("Mode must be 'dummy' or 'real'.")
}
outcome <- opt$outcome
num_groups <- opt$num

results_dir <- file.path("data_work", "output", mode, "results")
results_path <- file.path(results_dir, sprintf("main_%s.csv", outcome))
if (!file.exists(results_path)) {
  stop(sprintf("Results file '%s' not found. Run the pipeline first.", results_path))
}

results <- read.csv(results_path, stringsAsFactors = FALSE)
results <- results[order(results$flow, results$partner, results$base_year,
                         results$period, results$iv_label), ]

group_keys <- unique(results[, c("flow", "partner")])
if (nrow(group_keys) == 0) {
  stop("No specifications found for the requested outcome.")
}
group_keys <- head(group_keys, num_groups)

if (!requireNamespace("xtable", quietly = TRUE)) {
  stop("Package 'xtable' is required. Install it before running this script.")
}
suppressPackageStartupMessages(library(xtable))

sanitize_tex <- function(x) {
  x <- gsub("\\\\", "\\\\textbackslash{}", x)
  x <- gsub("_", "\\\\_", x, fixed = TRUE)
  x <- gsub("%", "\\\\%", x, fixed = TRUE)
  x <- gsub("#", "\\\\#", x, fixed = TRUE)
  x <- gsub("&", "\\\\&", x, fixed = TRUE)
  x <- gsub("\\{", "\\\\{", x)
  x <- gsub("\\}", "\\\\}", x)
  x
}

format_coef <- function(beta, se) {
  if (is.na(beta) || is.na(se)) {
    return(c(coef = NA_character_, se = NA_character_))
  }
  if (se == 0) {
    stars <- ""
  } else {
    t_stat <- beta / se
    p_val <- 2 * (1 - pnorm(abs(t_stat)))
    stars <- if (p_val < 0.001) {
      "***"
    } else if (p_val < 0.01) {
      "**"
    } else if (p_val < 0.05) {
      "*"
    } else {
      ""
    }
  }
  coef_str <- sprintf("%.3f%s", beta, stars)
  se_str <- sprintf("(%.3f)", se)
  c(coef = coef_str, se = se_str)
}

build_table <- function(spec_subset, outcome, mode) {
  spec_subset <- spec_subset[order(spec_subset$base_year, spec_subset$period), ]
  base_years <- subset(spec_subset, select = "base_year")[, 1]
  periods <- subset(spec_subset, select = "period")[, 1]

  formatted <- t(mapply(format_coef, spec_subset$coef, spec_subset$se))

  table_rows <- list()
  table_rows[[length(table_rows) + 1]] <- c("Exposure", formatted[, "coef"])
  table_rows[[length(table_rows) + 1]] <- c("", formatted[, "se"])
  table_rows[[length(table_rows) + 1]] <- c("Observations", spec_subset$N)
  table_rows[[length(table_rows) + 1]] <- c("Clusters", spec_subset$clusters)
  table_rows[[length(table_rows) + 1]] <- c("Base year", spec_subset$base_year)
  table_rows[[length(table_rows) + 1]] <- c("Period", gsub("_", "–", spec_subset$period))
  table_rows[[length(table_rows) + 1]] <- c("Instrument", spec_subset$iv_label)
  table_rows[[length(table_rows) + 1]] <- c("Fixed effects", spec_subset$fe_flags)

  table_matrix <- do.call(rbind, table_rows)
  col_labels <- paste0("(", seq_len(nrow(spec_subset)), ") ",
                       spec_subset$base_year, ", ",
                       gsub("_", "–", spec_subset$period))

  table_df <- data.frame(table_matrix, stringsAsFactors = FALSE)
  colnames(table_df) <- c("Variables", col_labels)

  xt <- xtable(table_df,
               caption = sanitize_tex(sprintf("%s exposure results", spec_subset$partner[1])),
               label = sanitize_tex(sprintf("tab:%s_%s_%s", outcome,
                                            spec_subset$flow[1], spec_subset$partner[1])))
  align(xt) <- c("l", "p{4cm}", rep(">{\\centering\\arraybackslash}p{2.6cm}",
                                    ncol(table_df) - 1))

  xt
}

tables_dir <- file.path("data_work", "output", mode, "tables")
dir.create(tables_dir, recursive = TRUE, showWarnings = FALSE)

pdf_outputs <- character(0)

for (idx in seq_len(nrow(group_keys))) {
  flow_key <- group_keys$flow[idx]
  partner_key <- group_keys$partner[idx]

  subset_specs <- results[results$flow == flow_key &
                            results$partner == partner_key, ]
  if (nrow(subset_specs) == 0) next

  xt <- build_table(subset_specs, outcome, mode)

  tex_file <- file.path(tables_dir,
                        sprintf("%s_%s_%s.tex", outcome, flow_key, partner_key))
  print(xt, file = tex_file, include.rownames = FALSE, booktabs = TRUE,
        sanitize.text.function = sanitize_tex, floating = TRUE)

  doc_tex <- sprintf(
"\\documentclass[10pt]{article}
\\usepackage[margin=0.75in]{geometry}
\\usepackage{booktabs}
\\usepackage{array}
\\usepackage{pdflscape}
\\begin{document}
\\begin{landscape}
\\scriptsize
\\centering
\\input{%s}
\\end{landscape}
\\end{document}
",
    basename(tex_file)
  )

  doc_path <- file.path(tables_dir,
                        sprintf("%s_%s_%s_doc.tex", outcome, flow_key, partner_key))
  writeLines(doc_tex, doc_path)

  old_wd <- getwd()
  setwd(tables_dir)
  pdf_file <- sprintf("%s_%s_%s_doc.pdf", outcome, flow_key, partner_key)
  tryCatch(
    {
      tools::texi2pdf(basename(doc_path), clean = TRUE, quiet = TRUE)
      pdf_outputs <- c(pdf_outputs, normalizePath(pdf_file, mustWork = FALSE))
    },
    error = function(e) {
      warning(sprintf("Failed to compile PDF for %s/%s: %s",
                      flow_key, partner_key, conditionMessage(e)))
    },
    finally = {
      setwd(old_wd)
    }
  )
}

if (length(pdf_outputs) > 0) {
  message("Generated tables:")
  for (p in pdf_outputs) message(" - ", p)
} else {
  warning("No tables were generated. Check filters or results.")
}
