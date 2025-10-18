choose_iv_label <- function(partner) {
  mapping <- list(
    CHN = "chn_wld_minus_idn_lag1",
    USA = "usa_lag1",
    JPN = "jpn_lag1",
    WLD_minus_IDN = "chn_wld_minus_idn_lag1"
  )
  mapping[[partner]]
}

run_2sls <- function(data, outcome_var, exposure_var, instrument_var, controls, cluster_var, fe_spec) {
  fe_map <- c(district = "district_bps_2000")
  fe_columns <- if (length(fe_spec) > 0) {
    unname(ifelse(fe_spec %in% names(fe_map), fe_map[fe_spec], fe_spec))
  } else {
    character(0)
  }

  required <- unique(c(
    outcome_var,
    exposure_var,
    instrument_var,
    controls,
    cluster_var,
    fe_columns,
    "district_bps_2000",
    "year"
  ))

  data <- data[stats::complete.cases(data[, required, drop = FALSE]), ]
  if (nrow(data) < 30) {
    return(NULL)
  }

  cluster_formula <- stats::as.formula(paste0("~", cluster_var))

  fe_clause <- if (length(fe_columns) > 0) {
    paste(fe_columns, collapse = " + ")
  } else {
    "0"
  }

  rhs_terms <- if (length(controls) > 0) {
    paste(controls, collapse = " + ")
  } else {
    "1"
  }
  main_formula <- stats::as.formula(paste(
    outcome_var, "~", rhs_terms,
    "|", fe_clause,
    "|", paste(exposure_var, "~", instrument_var)
  ))

  iv_model <- tryCatch(
    fixest::feols(
      fml = main_formula,
      data = data,
      cluster = cluster_formula
    ),
    error = function(e) {
      warning(sprintf("IV estimation failed for outcome '%s': %s", outcome_var, e$message))
      return(NULL)
    }
  )
  if (is.null(iv_model)) {
    return(NULL)
  }

  coef_table <- fixest::coeftable(iv_model)
  coef_name <- exposure_var
  if (!coef_name %in% rownames(coef_table)) {
    coef_name <- paste0("iv(", exposure_var, ")")
  }
  if (!coef_name %in% rownames(coef_table)) {
    coef_name <- paste0("fit_", exposure_var)
  }
  if (!coef_name %in% rownames(coef_table)) {
    return(NULL)
  }

  beta <- coef_table[coef_name, "Estimate"]
  se <- coef_table[coef_name, "Std. Error"]

  fs_rhs_terms <- c(instrument_var, controls)
  fs_rhs <- if (length(fs_rhs_terms) > 0) {
    paste(fs_rhs_terms, collapse = " + ")
  } else {
    "1"
  }
  fs_formula <- stats::as.formula(paste(
    exposure_var, "~", fs_rhs, "|", fe_clause
  ))
  fs_model <- tryCatch(
    fixest::feols(
      fml = fs_formula,
      data = data,
      cluster = cluster_formula
    ),
    error = function(e) NULL
  )

  fs_f <- NA_real_
  r2_first <- NA_real_
  if (!is.null(fs_model)) {
    fs_table <- fixest::coeftable(fs_model)
    if (instrument_var %in% rownames(fs_table)) {
      fs_f <- tryCatch({
        wald_obj <- NULL
        capture.output(
          wald_obj <- fixest::wald(fs_model, instrument_var)
        )
        wald_obj$stat
      },
      error = function(e) NA_real_)
    }
    r2_first <- suppressWarnings(as.numeric(fixest::fitstat(fs_model, "r2")))
  }

  fe_desc <- if (fe_clause == "0") "none" else gsub("\\s+", "", fe_clause)

  list(
    beta = beta,
    se = se,
    N = nrow(data),
    clusters = length(unique(data[[cluster_var]])),
    fe_flags = fe_desc,
    f_partial = fs_f,
    r2_first = r2_first
  )
}

estimate_grid <- function(panel, params) {
  outcomes <- params$analysis$outcomes
  flows <- params$analysis$flows
  partners <- params$analysis$estimation_partners
  if (is.null(partners) || length(partners) == 0) {
    partners <- params$analysis$partners
  }
  base_years <- params$analysis$base_years
  periods <- params$analysis$periods
  controls <- c("urban_share", "share_hs_or_above", "literacy_rate")
  cluster_var <- params$analysis$estimation$cluster
  fe_spec <- params$analysis$estimation$fe

  transform_specs <- list(
    list(
      id = "level_t",
      label = "Level (y_t, x_t)",
      outcome_suffix = "level",
      exposure_var = "exposure_level",
      instrument_var = "instrument_level_lag1",
      x_timing = "t"
    ),
    list(
      id = "diff_t",
      label = "Δ Level (y_t, x_t)",
      outcome_suffix = "diff",
      exposure_var = "exposure_level_diff",
      instrument_var = "instrument_level_diff_lag1",
      x_timing = "t"
    ),
    list(
      id = "difflag_t",
      label = "Δ Level lag (y_{t-1}, x_{t-1})",
      outcome_suffix = "diff_lag1",
      exposure_var = "exposure_level_diff_lag1",
      instrument_var = "instrument_level_diff_lag1",
      x_timing = "t"
    ),
    list(
      id = "log_t",
      label = "Log (y_t, x_t)",
      outcome_suffix = "log",
      exposure_var = "exposure_log",
      instrument_var = "instrument_log_lag1",
      x_timing = "t"
    ),
    list(
      id = "logdiff_t",
      label = "Δ Log (y_t, x_t)",
      outcome_suffix = "log_diff",
      exposure_var = "exposure_log_diff",
      instrument_var = "instrument_log_diff_lag1",
      x_timing = "t"
    ),
    list(
      id = "logdifflag_t",
      label = "Δ Log lag (y_{t-1}, x_{t-1})",
      outcome_suffix = "log_diff_lag1",
      exposure_var = "exposure_log_diff_lag1",
      instrument_var = "instrument_log_diff_lag1",
      x_timing = "t"
    ),
    list(
      id = "level_xtlag",
      label = "Level (y_t, x_{t-1})",
      outcome_suffix = "level",
      exposure_var = "exposure_level_lag1",
      instrument_var = "instrument_level_lag1",
      x_timing = "t-1"
    ),
    list(
      id = "diff_xtlag",
      label = "Δ Level (y_t, x_{t-1})",
      outcome_suffix = "diff",
      exposure_var = "exposure_level_diff_lag1",
      instrument_var = "instrument_level_diff_lag1",
      x_timing = "t-1"
    ),
    list(
      id = "difflag_xtlag",
      label = "Δ Level lag (y_{t-1}, x_{t-2})",
      outcome_suffix = "diff_lag1",
      exposure_var = "exposure_level_diff_lag2",
      instrument_var = "instrument_level_diff_lag2",
      x_timing = "t-2"
    ),
    list(
      id = "log_xtlag",
      label = "Log (y_t, x_{t-1})",
      outcome_suffix = "log",
      exposure_var = "exposure_log_lag1",
      instrument_var = "instrument_log_lag1",
      x_timing = "t-1"
    ),
    list(
      id = "logdiff_xtlag",
      label = "Δ Log (y_t, x_{t-1})",
      outcome_suffix = "log_diff",
      exposure_var = "exposure_log_diff_lag1",
      instrument_var = "instrument_log_diff_lag1",
      x_timing = "t-1"
    ),
    list(
      id = "logdifflag_xtlag",
      label = "Δ Log lag (y_{t-1}, x_{t-2})",
      outcome_suffix = "log_diff_lag1",
      exposure_var = "exposure_log_diff_lag2",
      instrument_var = "instrument_log_diff_lag2",
      x_timing = "t-2"
    )
  )

  main_results <- list()
  first_stage_results <- list()
  idx_main <- 1L
  idx_first <- 1L

  for (outcome in outcomes) {
    for (flow in flows) {
      for (partner in partners) {
        iv_label <- choose_iv_label(partner)
        if (is.null(iv_label)) {
          next
        }
        for (base_year in base_years) {
          subset_panel <- panel[
            panel$flow_code == flow &
              panel$partner_iso3 == partner &
              panel$base_year == base_year &
              panel$iv_label == iv_label,
            ,
            drop = FALSE
          ]

          if (nrow(subset_panel) == 0) {
            next
          }

          for (period_label in periods) {
            bounds <- period_to_bounds(period_label)
            period_data <- subset_panel[
              subset_panel$year >= bounds[1] &
                subset_panel$year <= bounds[2],
              ,
              drop = FALSE
            ]

            if (nrow(period_data) == 0) {
              next
            }

            for (transform in transform_specs) {
              outcome_var <- paste0(outcome, "_", transform$outcome_suffix)
              if (!all(c(outcome_var, transform$exposure_var, transform$instrument_var) %in% names(period_data))) {
                next
              }

              result <- run_2sls(
                data = period_data,
                outcome_var = outcome_var,
                exposure_var = transform$exposure_var,
                instrument_var = transform$instrument_var,
                controls = controls,
                cluster_var = cluster_var,
                fe_spec = fe_spec
              )

              if (is.null(result)) {
                next
              }

              table_id <- paste(
                outcome,
                flow,
                partner,
                base_year,
                iv_label,
                period_label,
                transform$id,
                sep = "__"
              )

              main_results[[idx_main]] <- data.frame(
                table_id = table_id,
                outcome = outcome,
                outcome_var = outcome_var,
                flow = flow,
                partner = partner,
                base_year = base_year,
                period = period_label,
                iv_label = iv_label,
                transform_id = transform$id,
                transform_label = transform$label,
                exposure_var = transform$exposure_var,
                instrument_var = transform$instrument_var,
                x_timing = transform$x_timing,
                coef = result$beta,
                se = result$se,
                N = result$N,
                clusters = result$clusters,
                fe_flags = result$fe_flags,
                stringsAsFactors = FALSE
              )
              idx_main <- idx_main + 1L

              first_stage_results[[idx_first]] <- data.frame(
                outcome_x = paste(outcome, flow, partner, base_year, transform$id, sep = "__"),
                instrument = iv_label,
                transform_id = transform$id,
                transform_label = transform$label,
                exposure_var = transform$exposure_var,
                instrument_var = transform$instrument_var,
                F_partial = result$f_partial,
                R2_first = result$r2_first,
                N = result$N,
                period = period_label,
                stringsAsFactors = FALSE
              )
              idx_first <- idx_first + 1L
            }
          }
        }
      }
    }
  }

  list(
    main = if (length(main_results) > 0) do.call(rbind, main_results) else data.frame(),
    first_stage = if (length(first_stage_results) > 0) do.call(rbind, first_stage_results) else data.frame()
  )
}
