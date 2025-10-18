#!/usr/bin/env Rscript

# Verbose R orchestrator for the China Shift-Share dummy pipeline.
# Provides step-by-step logging with timings and artifact locations.

DetectScriptPath <- function() {
  args <- commandArgs(trailingOnly = FALSE)
  file_arg <- "--file="
  match <- grep(file_arg, args)
  if (length(match) > 0) {
    return(normalizePath(sub(file_arg, "", args[match])))
  }
  stop("Unable to determine script path. Please run via Rscript scripts/R/orchestrate.R <command>.")
}

script_path <- DetectScriptPath()
repo_root <- normalizePath(file.path(dirname(script_path), "..", ".."))
setwd(repo_root)

source("scripts/R/lib/utils.R")
source("scripts/R/lib/config.R")
source("scripts/R/lib/dummy_generators.R")
source("scripts/R/lib/estimation.R")
source("scripts/R/lib/validation.R")

commands <- c("run", "list", "clean")
pipeline_steps <- c(
  "Load configuration",
  "Check required packages",
  "Prepare directory structure",
  "Generate dimensions",
  "Generate concordances",
  "Generate raw labor (SAKERNAS)",
  "Process labor facts",
  "Generate raw trade (HS)",
  "Process trade facts",
  "Compute national shocks",
  "Generate tariff series",
  "Compute tariff shocks",
  "Build exposures",
  "Build instruments",
  "Generate raw SUSENAS households",
  "Process SUSENAS outcomes",
  "Assemble analysis panel",
  "Estimate regression grid",
  "Persist processed artifacts",
  "Write results and diagnostics"
)

args <- commandArgs(trailingOnly = TRUE)
command <- if (length(args) > 0) args[[1]] else "run"

if (!command %in% commands) {
  stop(sprintf("Unknown command '%s'. Supported commands: %s", command, paste(commands, collapse = ", ")))
}

if (command == "list") {
  cat(paste(sprintf("%02d - %s", seq_along(pipeline_steps), pipeline_steps), collapse = "
"))
  quit(status = 0)
}

if (command == "clean") {
  config <- load_config("config")
  paths <- unlist(config$globals$paths, use.names = FALSE)
  for (p in paths) {
    if (dir.exists(p)) {
      log_info(sprintf("Cleaning directory: %s", normalizePath(p, mustWork = FALSE)))
      targets <- list.files(p, full.names = TRUE, all.files = TRUE, no.. = TRUE)
      unlink(targets, recursive = TRUE, force = TRUE)
    }
  }
  log_info("Clean complete. Re-run with 'Rscript scripts/R/orchestrate.R run'.")
  quit(status = 0)
}

run_pipeline <- function() {
  config <- load_config("config")
  globals <- config$globals
  params <- config$params

  mode <- tolower(globals$execution$mode)
  if (!mode %in% c("dummy", "real")) {
    stop(sprintf("Unsupported execution mode '%s'. Use 'dummy' or 'real'.", mode))
  }
  is_dummy_mode <- identical(mode, "dummy")

  raw_mode_dir <- file.path(globals$paths$raw, mode)
  raw_sakernas_dir <- file.path(raw_mode_dir, "sakernas")
  raw_trade_dir <- file.path(raw_mode_dir, "trade")
  raw_susenas_dir <- file.path(raw_mode_dir, "susenas")

  interim_mode_dir <- file.path(globals$paths$interim, mode)
  clean_sakernas_dir <- file.path(interim_mode_dir, "clean_sakernas")
  clean_trade_dir <- file.path(interim_mode_dir, "clean_trade")
  clean_susenas_dir <- file.path(interim_mode_dir, "clean_susenas")

  processed_mode_dir <- file.path(globals$paths$processed, mode)
  output_mode_dir <- file.path(globals$paths$output, mode)
  results_dir <- file.path(output_mode_dir, "results")
  diagnostics_dir <- file.path(output_mode_dir, "diagnostics")

  overall_start <- Sys.time()
  step_idx <- 0L
  step_label <- function(label) {
    step_idx <<- step_idx + 1L
    sprintf("Step %02d - %s", step_idx, label)
  }

  with_step(step_label("Check required packages"), {
    ensure_packages(c("jsonlite", "fixest"))
  })

  with_step(step_label("Prepare directory structure"), {
    if (is_dummy_mode) {
      ensure_directories(c(raw_sakernas_dir, raw_trade_dir, raw_susenas_dir))
    } else {
      ensure_non_empty_dir(raw_sakernas_dir, "Sakernas raw")
      ensure_non_empty_dir(raw_trade_dir, "Trade raw")
      ensure_non_empty_dir(raw_susenas_dir, "Susenas raw")
    }
    reset_directory(clean_sakernas_dir)
    reset_directory(clean_trade_dir)
    reset_directory(clean_susenas_dir)
    reset_directory(processed_mode_dir)
    reset_directory(results_dir)
    reset_directory(diagnostics_dir)
    ensure_directories(c(globals$paths$logs, globals$paths$meta))

    log_info(sprintf("Mode: %s", mode))
    log_info(sprintf("Data root: %s", normalizePath(globals$paths$data_root, mustWork = FALSE)))
    log_info(sprintf("Processed path: %s", normalizePath(processed_mode_dir, mustWork = FALSE)))
    log_info(sprintf("Output path: %s", normalizePath(results_dir, mustWork = FALSE)))
  })

  seed <- globals$execution$seed
  log_info(sprintf("Random seed (deterministic dummy data): %s", seed))

  dims <- with_step(step_label("Generate dimensions"), {
    dims_local <- generate_dimensions(params, seed)
    log_info(sprintf("    Years: %d | Districts: %d | Industries: %d | Partners: %d",
                    nrow(dims_local$dim_time_year),
                    nrow(dims_local$dim_geo_district_2000),
                    nrow(dims_local$dim_sector_isic_rev3),
                    nrow(dims_local$dim_partner)))
    dims_local
  })

  concordance <- with_step(step_label("Generate concordances"), {
    map <- generate_concordances(dims$dim_sector_isic_rev3, seed)
    log_info(sprintf("    HS→ISIC rows: %d", nrow(map)))
    map
  })

  years <- dims$dim_time_year$year

  sakernas_raw <- with_step(step_label("Generate raw labor (SAKERNAS)"), {
    if (is_dummy_mode) {
      raw_df <- generate_sakernas_raw(
        dim_geo = dims$dim_geo_district_2000,
        dim_sector = dims$dim_sector_isic_rev3,
        years = years,
        params = params,
        seed = seed
      )
      written <- write_partitioned_csv(raw_df, raw_sakernas_dir, "sakernas_micro", split_cols = "year")
      if (length(written) > 0) {
        log_info(sprintf("    SAKERNAS yearly files: %d (e.g., %s)", length(written), basename(written[1])))
      }
      raw_df
    } else {
      read_partitioned_csv(raw_sakernas_dir)
    }
  })

  labor_info <- with_step(step_label("Process labor facts"), {
    clean_prefix <- if (is_dummy_mode) "dummy_clean_sakernas" else "clean_sakernas"
    write_partitioned_csv(sakernas_raw, clean_sakernas_dir, clean_prefix, split_cols = "year")
    info <- process_sakernas_raw(
      sakernas_raw,
      dim_geo = dims$dim_geo_district_2000,
      dim_sector = dims$dim_sector_isic_rev3,
      params = params
    )
    log_info(sprintf("    Labor fact rows: %d", nrow(info$fact_labor)))
    info
  })

  trade_flows <- setdiff(params$analysis$flows, "tariff")

  trade_raw <- with_step(step_label("Generate raw trade (HS)"), {
    if (is_dummy_mode) {
      tr <- generate_trade_raw(
        dims$dim_sector_isic_rev3,
        params$analysis$partners,
        trade_flows,
        years,
        concordance,
        seed
      )
      written <- write_partitioned_csv(tr, raw_trade_dir, "trade_hs", split_cols = "year")
      if (length(written) > 0) {
        log_info(sprintf("    Trade yearly files: %d (e.g., %s)", length(written), basename(written[1])))
      }
      tr
    } else {
      read_partitioned_csv(raw_trade_dir)
    }
  })

  trade_flows <- setdiff(params$analysis$flows, "tariff")

  fact_trade <- with_step(step_label("Process trade facts"), {
    ft <- process_trade_raw(trade_raw, concordance)
    if (length(trade_flows) > 0) {
      ft <- ft[ft$flow_code %in% trade_flows, , drop = FALSE]
    }
    clean_prefix <- if (is_dummy_mode) "dummy_clean_trade_isic" else "clean_trade_isic"
    write_partitioned_csv(ft, clean_trade_dir, clean_prefix, split_cols = "year")
    log_info(sprintf("    Trade fact rows: %d", nrow(ft)))
    ft
  })

  national_shock <- with_step(step_label("Compute national shocks"), {
    shock <- compute_national_shocks(fact_trade)
    log_info(sprintf("    National shock rows: %d", nrow(shock)))
    shock
  })

  fact_tariff <- with_step(step_label("Generate tariff series"), {
    tf <- generate_tariff_series(
      dim_sector = dims$dim_sector_isic_rev3,
      partners = params$analysis$partners,
      years = years,
      seed = seed
    )
    log_info(sprintf("    Tariff fact rows: %d", nrow(tf)))
    tf
  })

  tariff_shock <- with_step(step_label("Compute tariff shocks"), {
    ts <- compute_tariff_shocks(fact_tariff)
    log_info(sprintf("    Tariff shock rows: %d", nrow(ts)))
    ts
  })

  exposures <- with_step(step_label("Build exposures"), {
    combined_shock <- rbind(national_shock, tariff_shock)
    exp_df <- generate_exposures(
      shock_df = combined_shock,
      trade_df = fact_trade,
      tariff_df = fact_tariff,
      labor_info = labor_info,
      dim_geo = dims$dim_geo_district_2000,
      dim_sector = dims$dim_sector_isic_rev3,
      params = params
    )
    log_info(sprintf("    Exposure rows: %d", nrow(exp_df)))
    exp_df
  })

  instruments <- with_step(step_label("Build instruments"), {
    inst <- generate_instruments(exposures, params)
    log_info(sprintf("    Instrument rows: %d", nrow(inst)))
    inst
  })

  susenas_raw <- with_step(step_label("Generate raw SUSENAS households"), {
    if (is_dummy_mode) {
      raw_df <- generate_susenas_raw(
        dim_geo = dims$dim_geo_district_2000,
        years = years,
        exposures = exposures,
        params = params,
        seed = seed
      )
      written <- write_partitioned_csv(raw_df, raw_susenas_dir, "susenas_micro", split_cols = "year")
      if (length(written) > 0) {
        log_info(sprintf("    SUSENAS yearly files: %d (e.g., %s)", length(written), basename(written[1])))
      }
      raw_df
    } else {
      read_partitioned_csv(raw_susenas_dir)
    }
  })

  outcomes <- with_step(step_label("Process SUSENAS outcomes"), {
    clean_prefix <- if (is_dummy_mode) "dummy_clean_susenas" else "clean_susenas"
    write_partitioned_csv(susenas_raw, clean_susenas_dir, clean_prefix, split_cols = "year")
    out_df <- process_susenas_raw(susenas_raw)
    log_info(sprintf("    Outcome rows: %d", nrow(out_df)))
    out_df
  })

  panel <- with_step(step_label("Assemble analysis panel"), {
    pnl <- assemble_panel(
      exposures = exposures,
      outcomes = outcomes,
      instruments = instruments,
      params = params
    )
    log_info(sprintf("    Panel rows: %d", nrow(pnl)))
    pnl
  })

  estimation_results <- with_step(step_label("Estimate regression grid"), {
    est <- estimate_grid(panel, params)
    log_info(sprintf("    Main specs: %d | First-stage specs: %d",
                    nrow(est$main), nrow(est$first_stage)))
    est
  })

  with_step(step_label("Persist processed artifacts"), {
    log_output_path("dim_time_year", write_csv_safe(dims$dim_time_year,
                   file.path(processed_mode_dir, "dim_time_year.csv")))
    log_output_path("dim_geo_district_2000", write_csv_safe(dims$dim_geo_district_2000,
                   file.path(processed_mode_dir, "dim_geo_district_2000.csv")))
    log_output_path("dim_sector_isic_rev3", write_csv_safe(dims$dim_sector_isic_rev3,
                   file.path(processed_mode_dir, "dim_sector_isic_rev3.csv")))
    log_output_path("dim_partner", write_csv_safe(dims$dim_partner,
                   file.path(processed_mode_dir, "dim_partner.csv")))

    log_output_path("map_hs_to_isic", write_csv_safe(concordance,
                   file.path(processed_mode_dir, "map_hs_to_isic.csv")))

    log_output_path("fact_labor_district_isic_year", write_csv_safe(labor_info$fact_labor,
                   file.path(processed_mode_dir, "fact_labor_district_isic_year.csv")))
    log_output_path("fact_trade_isic_partner_year_flow", write_csv_safe(fact_trade,
                   file.path(processed_mode_dir, "fact_trade_isic_partner_year_flow.csv")))
    log_output_path("fact_tariff_isic_partner_year", write_csv_safe(fact_tariff,
                   file.path(processed_mode_dir, "fact_tariff_isic_partner_year.csv")))
    log_output_path("fact_national_shock_isic_year", write_csv_safe(national_shock,
                   file.path(processed_mode_dir, "fact_national_shock_isic_year.csv")))
    log_output_path("fact_tariff_shock_isic_year", write_csv_safe(tariff_shock,
                   file.path(processed_mode_dir, "fact_tariff_shock_isic_year.csv")))
    log_output_path("fact_poverty_district_year", write_csv_safe(outcomes,
                   file.path(processed_mode_dir, "fact_poverty_district_year.csv")))
    log_output_path("exposure_district_year", write_csv_safe(exposures,
                   file.path(processed_mode_dir, "exposure_district_year.csv")))
    log_output_path("instrument_district_year", write_csv_safe(instruments,
                   file.path(processed_mode_dir, "instrument_district_year.csv")))
    log_output_path("panel_analysis", write_csv_safe(panel,
                   file.path(processed_mode_dir, "panel_analysis.csv")))
  })

  with_step(step_label("Write results and diagnostics"), {
    log_output_path("result_main_p0", write_csv_safe(
      estimation_results$main[estimation_results$main$outcome == "p0", , drop = FALSE],
      file.path(results_dir, "main_p0.csv")
    ))
    log_output_path("result_main_p1", write_csv_safe(
      estimation_results$main[estimation_results$main$outcome == "p1", , drop = FALSE],
      file.path(results_dir, "main_p1.csv")
    ))
    log_output_path("result_main_p2", write_csv_safe(
      estimation_results$main[estimation_results$main$outcome == "p2", , drop = FALSE],
      file.path(results_dir, "main_p2.csv")
    ))

    log_output_path("result_first_stage", write_csv_safe(
      estimation_results$first_stage,
      file.path(results_dir, "first_stage.csv")
    ))

    validation <- run_validations(labor_info, exposures, instruments, estimation_results, params)
    write_validation_report(
      validation,
      file.path(diagnostics_dir, "validation_report.json")
    )
    log_output_path("validation_report", file.path(diagnostics_dir, "validation_report.json"))
  })

  total_elapsed <- as.numeric(difftime(Sys.time(), overall_start, units = "secs"))
  log_info(sprintf("Pipeline finished successfully in %s", format_duration(total_elapsed)))
  log_info(sprintf("Processed artifacts available at: %s", normalizePath(processed_mode_dir, mustWork = FALSE)))
  log_info(sprintf("Results available at: %s", normalizePath(results_dir, mustWork = FALSE)))
  log_info(sprintf("Diagnostics available at: %s", normalizePath(diagnostics_dir, mustWork = FALSE)))
  log_info("Re-run with: Rscript scripts/R/orchestrate.R run")
}
run_pipeline()
