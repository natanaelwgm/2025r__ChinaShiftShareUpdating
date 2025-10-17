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
  overall_start <- Sys.time()
  step_idx <- 0L
  step_label <- function(label) {
    step_idx <<- step_idx + 1L
    sprintf("Step %02d - %s", step_idx, label)
  }

  config <- with_step(step_label("Load configuration"), load_config("config"))
  globals <- config$globals
  params <- config$params

  with_step(step_label("Check required packages"), {
    ensure_packages(c("jsonlite", "fixest"))
  })

  with_step(step_label("Prepare directory structure"), {
    ensure_directories(unlist(globals$paths, use.names = FALSE))
    ensure_directories(c(
      file.path(globals$paths$processed, "results"),
      file.path(globals$paths$output, "results"),
      file.path(globals$paths$output, "diagnostics"),
      file.path(globals$paths$raw, "sakernas"),
      file.path(globals$paths$raw, "trade"),
      file.path(globals$paths$raw, "susenas")
    ))
    log_info(sprintf("Data root: %s", normalizePath(globals$paths$data_root, mustWork = FALSE)))
    log_info(sprintf("Processed path: %s", normalizePath(globals$paths$processed, mustWork = FALSE)))
    log_info(sprintf("Output path: %s", normalizePath(globals$paths$output, mustWork = FALSE)))
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
  sakernas_bundle <- with_step(step_label("Generate raw labor (SAKERNAS)"), {
    bundle <- generate_sakernas_raw(
      dim_geo = dims$dim_geo_district_2000,
      dim_sector = dims$dim_sector_isic_rev3,
      years = years,
      params = params,
      seed = seed
    )
    raw_dir <- file.path(globals$paths$raw, "sakernas")
    written <- write_partitioned_csv(bundle$raw, raw_dir, "sakernas_micro", split_cols = "year")
    if (length(written) > 0) {
      log_info(sprintf("    SAKERNAS yearly files: %d (e.g., %s)", length(written), basename(written[1])))
    } else {
      log_info("    SAKERNAS yearly files: none generated")
    }
    legacy_path <- file.path(globals$paths$raw, "sakernas_micro.csv")
    if (file.exists(legacy_path)) unlink(legacy_path)
    bundle
  })

  labor_info <- with_step(step_label("Process labor facts"), {
    info <- process_sakernas_raw(
      sakernas_bundle,
      dims$dim_geo_district_2000,
      dims$dim_sector_isic_rev3
    )
    log_info(sprintf("    Labor fact rows: %d", nrow(info$fact_labor)))
    info
  })
  rm(sakernas_bundle)

  trade_raw <- with_step(step_label("Generate raw trade (HS)"), {
    tr <- generate_trade_raw(
      dims$dim_sector_isic_rev3,
      params$analysis$partners,
      params$analysis$flows,
      years,
      concordance,
      seed
    )
    log_info(sprintf("    Raw HS trade rows: %d", nrow(tr)))
    raw_dir <- file.path(globals$paths$raw, "trade")
    written <- write_partitioned_csv(tr, raw_dir, "trade_hs", split_cols = "year")
    if (length(written) > 0) {
      log_info(sprintf("    Trade yearly files: %d (e.g., %s)", length(written), basename(written[1])))
    } else {
      log_info("    Trade yearly files: none generated")
    }
    legacy_path <- file.path(globals$paths$raw, "trade_hs.csv")
    if (file.exists(legacy_path)) unlink(legacy_path)
    tr
  })

  fact_trade <- with_step(step_label("Process trade facts"), {
    ft <- process_trade_raw(trade_raw, concordance)
    log_info(sprintf("    Trade fact rows: %d", nrow(ft)))
    ft
  })
  rm(trade_raw)

  national_shock <- with_step(step_label("Compute national shocks"), {
    shock <- compute_national_shocks(fact_trade)
    log_info(sprintf("    National shock rows: %d", nrow(shock)))
    shock
  })

  exposures <- with_step(step_label("Build exposures"), {
    exp_df <- generate_exposures(
      shock_df = national_shock,
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
    raw_df <- generate_susenas_raw(
      dim_geo = dims$dim_geo_district_2000,
      years = years,
      exposures = exposures,
      params = params,
      seed = seed
    )
    log_info(sprintf("    SUSENAS household rows: %d", nrow(raw_df)))
    raw_dir <- file.path(globals$paths$raw, "susenas")
    written <- write_partitioned_csv(raw_df, raw_dir, "susenas_micro", split_cols = "year")
    if (length(written) > 0) {
      log_info(sprintf("    SUSENAS yearly files: %d (e.g., %s)", length(written), basename(written[1])))
    } else {
      log_info("    SUSENAS yearly files: none generated")
    }
    legacy_path <- file.path(globals$paths$raw, "susenas_micro.csv")
    if (file.exists(legacy_path)) unlink(legacy_path)
    raw_df
  })

  outcomes <- with_step(step_label("Process SUSENAS outcomes"), {
    out_df <- process_susenas_raw(susenas_raw)
    log_info(sprintf("    Outcome rows: %d", nrow(out_df)))
    out_df
  })
  rm(susenas_raw)

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
      artifact_path(globals, "processed", "dim_time_year.csv")))
    log_output_path("dim_geo_district_2000", write_csv_safe(dims$dim_geo_district_2000,
      artifact_path(globals, "processed", "dim_geo_district_2000.csv")))
    log_output_path("dim_sector_isic_rev3", write_csv_safe(dims$dim_sector_isic_rev3,
      artifact_path(globals, "processed", "dim_sector_isic_rev3.csv")))
    log_output_path("dim_partner", write_csv_safe(dims$dim_partner,
      artifact_path(globals, "processed", "dim_partner.csv")))
    log_output_path("map_hs_to_isic", write_csv_safe(concordance,
      artifact_path(globals, "processed", "map_hs_to_isic.csv")))
    log_output_path("fact_labor_district_isic_year", write_csv_safe(labor_info$fact_labor,
      artifact_path(globals, "processed", "fact_labor_district_isic_year.csv")))
    log_output_path("fact_trade_isic_partner_year_flow", write_csv_safe(fact_trade,
      artifact_path(globals, "processed", "fact_trade_isic_partner_year_flow.csv")))
    log_output_path("fact_national_shock_isic_year", write_csv_safe(national_shock,
      artifact_path(globals, "processed", "fact_national_shock_isic_year.csv")))
    log_output_path("fact_poverty_district_year", write_csv_safe(outcomes,
      artifact_path(globals, "processed", "fact_poverty_district_year.csv")))
    log_output_path("exposure_district_year", write_csv_safe(exposures,
      artifact_path(globals, "processed", "exposure_district_year.csv")))
    log_output_path("instrument_district_year", write_csv_safe(instruments,
      artifact_path(globals, "processed", "instrument_district_year.csv")))
    log_output_path("panel_analysis", write_csv_safe(panel,
      artifact_path(globals, "processed", "panel_analysis.csv")))
  })

  with_step(step_label("Write results and diagnostics"), {
    results_dir <- file.path(globals$paths$output, "results")
    diagnostics_dir <- file.path(globals$paths$output, "diagnostics")

    for (outcome in params$analysis$outcomes) {
      outcome_df <- estimation_results$main[estimation_results$main$outcome == outcome, , drop = FALSE]
      if (nrow(outcome_df) > 0) {
        log_output_path(sprintf("result_main_%s", outcome), write_csv_safe(
          outcome_df,
          file.path(results_dir, sprintf("main_%s.csv", outcome))
        ))
      } else {
        log_info(sprintf("    No regression rows for outcome '%s'", outcome))
      }
    }

    log_output_path("result_first_stage", write_csv_safe(
      estimation_results$first_stage,
      file.path(results_dir, "first_stage.csv")
    ))

    validation_path <- file.path(diagnostics_dir, "validation_report.json")
    report <- run_validations(labor_info, exposures, instruments, estimation_results)
    write_validation_report(report, validation_path)
    log_output_path("validation_report", validation_path)
  })

  total_elapsed <- as.numeric(difftime(Sys.time(), overall_start, units = "secs"))
  log_info(sprintf("Pipeline finished successfully in %s", format_duration(total_elapsed)))
  log_info(sprintf("Processed artifacts available at: %s", normalizePath(globals$paths$processed, mustWork = FALSE)))
  log_info(sprintf("Results available at: %s", normalizePath(file.path(globals$paths$output, "results"), mustWork = FALSE)))
  log_info(sprintf("Diagnostics available at: %s", normalizePath(file.path(globals$paths$output, "diagnostics"), mustWork = FALSE)))
  log_info("Re-run with: Rscript scripts/R/orchestrate.R run")
}

if (command == "run") {
  run_pipeline()
}
