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
              help = "Outcome to export (p0, p1, p2). [default %default]")
)

opt <- parse_args(OptionParser(option_list = option_list))

mode <- tolower(opt$mode)
if (!mode %in% c("dummy", "real")) {
  stop("Mode must be 'dummy' or 'real'.")
}
outcome <- opt$outcome

results_dir <- file.path("data_work", "output", mode, "results")
if (!dir.exists(results_dir)) {
  stop(sprintf("Results directory '%s' not found. Run the pipeline first.", results_dir))
}

main_path <- file.path(results_dir, sprintf("main_%s.csv", outcome))
if (!file.exists(main_path)) {
  stop(sprintf("Results file '%s' not found.", main_path))
}

results <- read.csv(main_path, stringsAsFactors = FALSE)

subset_specs <- subset(
  results,
  flow == "import" &
    partner == "CHN" &
    base_year %in% c(2000, 2008) &
    period %in% c("2000_2007", "2008_2012", "2000_2015")
)

if (nrow(subset_specs) == 0) {
  stop("No specifications matched the default selection. Adjust the filter in export_tables.R.")
}

subset_specs$spec_id <- paste0(
  "Base ", subset_specs$base_year, ", ",
  gsub("_", "–", subset_specs$period)
)

subset_specs$column_label <- paste0(
  "(", seq_len(nrow(subset_specs)), ") ", subset_specs$spec_id
)

format_coef <- function(beta, se) {
  t_stat <- ifelse(se == 0, NA, beta / se)
  p_val <- ifelse(is.na(t_stat), NA, 2 * pnorm(-abs(t_stat)))
  stars <- ifelse(is.na(p_val), "",
                  ifelse(p_val < 0.001, "***",
                         ifelse(p_val < 0.01, "**",
                                ifelse(p_val < 0.05, "*", ""))))
  coef_str <- sprintf("%.3f%s", beta, stars)
  se_str <- sprintf("(%.3f)", se)
  c(coef = coef_str, se = se_str)
}

coef_rows <- t(mapply(format_coef, subset_specs$coef, subset_specs$se))
coef_vector <- as.vector(t(coef_rows))

table_rows <- list()
table_rows[[length(table_rows) + 1]] <- c("Exposure", coef_rows[, "coef"])
table_rows[[length(table_rows) + 1]] <- c(" ", coef_rows[, "se"])
table_rows[[length(table_rows) + 1]] <- c("Observations", as.character(subset_specs$N))
table_rows[[length(table_rows) + 1]] <- c("Clusters", as.character(subset_specs$clusters))
table_rows[[length(table_rows) + 1]] <- c("Base year", as.character(subset_specs$base_year))
table_rows[[length(table_rows) + 1]] <- c("Period", gsub("_", "–", subset_specs$period))
table_rows[[length(table_rows) + 1]] <- c("IV", subset_specs$iv_label)
table_rows[[length(table_rows) + 1]] <- c("FE (district, year)", subset_specs$fe_flags)

table_matrix <- do.call(rbind, table_rows)
table_df <- as.data.frame(table_matrix, stringsAsFactors = FALSE)
colnames(table_df) <- c("Variables", subset_specs$column_label)

tables_dir <- file.path("data_work", "output", mode, "tables")
if (!dir.exists(tables_dir)) dir.create(tables_dir, recursive = TRUE, showWarnings = FALSE)

latex_table_path <- file.path(tables_dir, sprintf("%s_import_CHN.tex", outcome))
latex_doc_path <- file.path(tables_dir, sprintf("%s_import_CHN_doc.tex", outcome))


if (!requireNamespace("xtable", quietly = TRUE)) {
  stop("Package 'xtable' is required. Install it before running this script.")
}

suppressPackageStartupMessages(library(xtable))
latex_body <- xtable(table_df,
  caption = sprintf("Impact of Import Exposure on %s (selected specs)", toupper(outcome)),
  label = sprintf("tab:%s_import_spec", outcome)
)

col_count <- ncol(table_df)
align_vec <- c("p{4cm}", rep(">{\\centering\\arraybackslash}p{2.6cm}", col_count - 1))
align(latex_body) <- c("p{4cm}", align_vec)

sanitize <- function(x) gsub('_', '\\_', x, fixed = TRUE)
print(latex_body, file = latex_table_path, include.rownames = FALSE, booktabs = TRUE,
      sanitize.text.function = sanitize, floating = FALSE)

latex_doc <- sprintf(
"\\documentclass[10pt]{article}
\\usepackage[margin=0.75in]{geometry}
\\usepackage{booktabs}
\\usepackage{array}
\\usepackage{pdflscape}
\\usepackage{graphicx}
\\usepackage{caption}
\\begin{document}
\\begin{landscape}
\\scriptsize
\\input{%s}
\\captionof{table}{Impact of Import Exposure on %s (selected specifications)}
\\end{landscape}
\\end{document}
",
  basename(latex_table_path),
  toupper(outcome)
)
writeLines(latex_doc, con = latex_doc_path)

old_wd <- getwd()
setwd(tables_dir)
tex_file <- basename(latex_doc_path)
pdf_file <- sprintf("%s.pdf", tools::file_path_sans_ext(tex_file))
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

pdf_abspath <- normalizePath(file.path(tables_dir, pdf_file), mustWork = FALSE)

if (success && file.exists(pdf_abspath)) {
  message(sprintf("Exported LaTeX table to %s", normalizePath(latex_table_path)))
  message(sprintf("Exported PDF table to %s", pdf_abspath))
} else {
  warning("PDF was not generated. Check LaTeX logs for details.")
}
