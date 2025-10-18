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
  make_option(c("-n", "--num"), type = "integer", default = 10,
              help = "Number of specifications to include. [default %default]")
)

opt <- parse_args(OptionParser(option_list = option_list))

mode <- tolower(opt$mode)
if (!mode %in% c("dummy", "real")) {
  stop("Mode must be 'dummy' or 'real'.")
}
outcome <- opt$outcome
num_specs <- opt$num

results_dir <- file.path("data_work", "output", mode, "results")
if (!dir.exists(results_dir)) {
  stop(sprintf("Results directory '%s' not found. Run the pipeline first.", results_dir))
}

main_path <- file.path(results_dir, sprintf("main_%s.csv", outcome))
if (!file.exists(main_path)) {
  stop(sprintf("Results file '%s' not found.", main_path))
}

results <- read.csv(main_path, stringsAsFactors = FALSE)
results <- results[order(results$flow, results$partner, results$base_year,
                         results$period, results$iv_label), ]

subset_specs <- head(results, num_specs)

if (nrow(subset_specs) == 0) {
  stop("No specifications available for the requested outcome.")
}

if (!requireNamespace("xtable", quietly = TRUE)) {
  stop("Package 'xtable' is required. Install it before running this script.")
}
suppressPackageStartupMessages(library(xtable))

format_coef <- function(beta, se) {
  t_stat <- ifelse(se == 0, NA, beta / se)
  p_val <- ifelse(is.na(t_stat), NA, 2 * pnorm(-abs(t_stat)))
  stars <- ifelse(is.na(p_val), "",
                  ifelse(p_val < 0.001, "***",
                         ifelse(p_val < 0.01, "**",
                                ifelse(p_val < 0.05, "*", ""))))
  coef_str <- sprintf("%.3f%s", beta, stars)
  se_str <- sprintf("(%.3f)", se)
  list(coef = coef_str, se = se_str)
}

sanitize_text <- function(x) {
  replacements <- c(
    "\\" = "\\\\textbackslash{}",
    "_" = "\\\\textunderscore{}",
    "%" = "\\%",
    "#" = "\\#",
    "&" = "\\&",
    "{" = "\\{",
    "}" = "\\}",
    "$" = "\\$",
    "~" = "\\\\textasciitilde{}",
    "^" = "\\\\textasciicircum{}"
  )
  result <- as.character(x)
  for (old in names(replacements)) {
    result <- gsub(old, replacements[[old]], result, fixed = TRUE)
  }
  result
}

read_cluster_label <- function() {
  config_path <- file.path("config", "params.yaml")
  if (!file.exists(config_path)) {
    return("district_bps_2000")
  }
  lines <- readLines(config_path, warn = FALSE)
  pattern <- "^\\s*cluster\\s*:\\s*(.+)\\s*$"
  idx <- grep(pattern, lines)
  if (length(idx) == 0) {
    return("district_bps_2000")
  }
  label <- trimws(sub(pattern, "\\1", lines[idx[1]]))
  if (nzchar(label)) label else "district_bps_2000"
}
sanitize_label <- function(x) {
  gsub("[^A-Za-z0-9]+", "_", x)
}

tables_dir <- file.path("data_work", "output", mode, "tables")
if (!dir.exists(tables_dir)) dir.create(tables_dir, recursive = TRUE, showWarnings = FALSE)

tex_path <- file.path(tables_dir, sprintf("%s_specbook.tex", outcome))
pdf_path <- file.path(tables_dir, sprintf("%s_specbook.pdf", outcome))

doc_lines <- c(
  "\\documentclass[12pt]{article}",
  "\\usepackage[margin=1in]{geometry}",
  "\\usepackage{booktabs}",
  "\\usepackage{pdflscape}",
  "\\usepackage{longtable}",
  "\\usepackage{caption}",
  "\\usepackage{array}",
  "\\begin{document}",
  sprintf("\\section*{Specification Booklet: Outcome %s (mode: %s)}", toupper(outcome), mode),
  "\\tableofcontents",
  "\\newpage"
)

summary_rows <- list()
cluster_label <- read_cluster_label()

for (idx in seq_len(nrow(subset_specs))) {
  spec <- subset_specs[idx, ]
  label <- sprintf("Spec %02d", idx)
  spec_title <- sprintf("%s: %s %s exposure, partner %s, base %d, period %s, IV %s",
                        label,
                        spec$flow,
                        spec$outcome,
                        spec$partner,
                        spec$base_year,
                        spec$period,
                        spec$iv_label)
  spec_title <- sanitize_text(spec_title)

  doc_lines <- c(doc_lines,
                 sprintf("\\section*{%s}", spec_title),
                 sprintf("\\label{sec:%s}", sanitize_label(label)),
                 sprintf("This page marks the start of %s.", label),
                 "\\newpage")

  coef_info <- format_coef(spec$coef, spec$se)

  table_df <- data.frame(
    Variable = c("Exposure coefficient", "Standard error", "Observations",
                 "Clusters", "Flow", "Partner", "Base year", "Period",
                 "Instrument", "Outcome", "Fixed effects", "Cluster level"),
    Value = c(coef_info$coef,
              coef_info$se,
              spec$N,
              spec$clusters,
              spec$flow,
              spec$partner,
              spec$base_year,
              gsub("_", "--", spec$period),
              spec$iv_label,
              spec$outcome,
              spec$fe_flags,
              cluster_label),
    stringsAsFactors = FALSE
  )

  value_lines <- paste0(sanitize_text(table_df$Variable),
                        " & ", sanitize_text(table_df$Value), " \\\")
  table_block <- c(
    "\\begin{table}[htbp]",
    "\\centering",
    "\\begin{tabular}{p{5cm}p{9cm}}",
    "\\toprule",
    "Variable & Value \\\",
    "\\midrule",
    value_lines,
    "\\bottomrule",
    "\\end{tabular}",
    sprintf("\\caption{%s}", sanitize_text(sprintf("%s detail table", label))),
    sprintf("\\label{%s}", sanitize_label(sprintf("tab_%s", label))),
    "\\end{table}"
  )

  doc_lines <- c(doc_lines,
                 table_block,
                 "\\newpage",
                 sprintf("End of %s.", label),
                 "\\newpage")

  summary_rows[[length(summary_rows) + 1]] <- data.frame(
    Spec = label,
    Outcome = spec$outcome,
    Flow = spec$flow,
    Partner = spec$partner,
    BaseYear = spec$base_year,
    Period = spec$period,
    Instrument = spec$iv_label,
    stringsAsFactors = FALSE
  )
}

summary_df <- do.call(rbind, summary_rows)
summary_xtab <- xtable(summary_df,
                       caption = sanitize_text("Summary of specifications included"),
                       label = sanitize_text(sprintf("tab:%s_summary", outcome)))
align(summary_xtab) <- c("l", rep("p{2.5cm}", ncol(summary_df)))
summary_tex <- capture.output(print(summary_xtab, include.rownames = FALSE,
                                    sanitize.text.function = sanitize_text,
                                    floating = TRUE))

doc_lines <- c(doc_lines,
               "\\section*{Specification Summary}",
               summary_tex,
               "\\end{document}")

writeLines(doc_lines, tex_path)

old_wd <- getwd()
setwd(tables_dir)
tex_file <- basename(tex_path)
pdf_file <- sprintf("%s_specbook.pdf", outcome)
success <- FALSE
tryCatch(
  {
    tools::texi2pdf(tex_file, clean = TRUE, quiet = TRUE)
    success <- TRUE
  },
  error = function(e) {
    warning(sprintf("LaTeX compilation failed: %s", conditionMessage(e)))
  },
  finally = {
    setwd(old_wd)
  }
)

if (success && file.exists(pdf_path)) {
  message(sprintf("Specification booklet written to %s", normalizePath(pdf_path)))
} else {
  warning("Specification booklet PDF was not generated. Check LaTeX logs for details.")
}
