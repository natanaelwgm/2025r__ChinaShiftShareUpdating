choose_iv_label <- function(partner) {
  mapping <- list(
    CHN = "chn_wld_minus_idn_lag1",
    USA = "usa_lag1",
    JPN = "jpn_lag1",
    WLD_minus_IDN = "chn_wld_minus_idn_lag1"
  )
  mapping[[partner]]
}

run_2sls <- function(data, outcome_var, controls, cluster_var) {
  required <- unique(c(
    outcome_var,
    "exposure_log_diff",
    "instrument_log_diff_lag1",
    controls,
    cluster_var,
    "district_bps_2000",
    "year"
  ))

  data <- data[stats::complete.cases(data[, required, drop = FALSE]), ]
  if (nrow(data) < 30) {
    return(NULL)
  }

  cluster_formula <- stats::as.formula(paste0("~", cluster_var))

  rhs_terms <- if (length(controls) > 0) {
    paste(controls, collapse = " + ")
  } else {
    "1"
  }
  main_formula <- stats::as.formula(paste(
    outcome_var, "~", rhs_terms,
    "| district_bps_2000 + year",
    "| exposure_log_diff ~ instrument_log_diff_lag1"
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
  coef_name <- "exposure_log_diff"
  if (!coef_name %in% rownames(coef_table)) {
    coef_name <- "iv(exposure_log_diff)"
  }
  if (!coef_name %in% rownames(coef_table)) {
    coef_name <- "fit_exposure_log_diff"
  }
  if (!coef_name %in% rownames(coef_table)) {
    return(NULL)
  }

  beta <- coef_table[coef_name, "Estimate"]
  se <- coef_table[coef_name, "Std. Error"]

  fs_rhs <- paste(c("instrument_log_diff_lag1", controls), collapse = " + ")
  fs_formula <- stats::as.formula(paste(
    "exposure_log_diff ~", fs_rhs, "| district_bps_2000 + year"
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
    if ("instrument_log_diff_lag1" %in% rownames(fs_table)) {
      fs_f <- tryCatch({
        wald_obj <- NULL
        capture.output(
          wald_obj <- fixest::wald(fs_model, "instrument_log_diff_lag1")
        )
        wald_obj$stat
      },
      error = function(e) NA_real_)
    }
    r2_first <- suppressWarnings(as.numeric(fixest::fitstat(fs_model, "r2")))
  }

  list(
    beta = beta,
    se = se,
    N = nrow(data),
    clusters = length(unique(data[[cluster_var]])),
    fe_flags = "district+year",
    f_partial = fs_f,
    r2_first = r2_first
  )
}

estimate_grid <- function(panel, params) {
  outcomes <- params$analysis$outcomes
  flows <- params$analysis$flows
  partners <- params$analysis$partners
  base_years <- params$analysis$base_years
  periods <- params$analysis$periods
  controls <- c("urban_share", "share_hs_or_above", "literacy_rate")

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

            result <- run_2sls(
              data = period_data,
              outcome_var = outcome,
              controls = controls,
              cluster_var = params$analysis$estimation$cluster
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
              sep = "__"
            )

            main_results[[idx_main]] <- data.frame(
              table_id = table_id,
              outcome = outcome,
              flow = flow,
              partner = partner,
              base_year = base_year,
              period = period_label,
              coef = result$beta,
              se = result$se,
              N = result$N,
              clusters = result$clusters,
              fe_flags = result$fe_flags,
              iv_label = iv_label,
              stringsAsFactors = FALSE
            )
            idx_main <- idx_main + 1L

            first_stage_results[[idx_first]] <- data.frame(
              outcome_x = paste(outcome, flow, partner, base_year, sep = "__"),
              instrument = iv_label,
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

  list(
    main = if (length(main_results) > 0) do.call(rbind, main_results) else data.frame(),
    first_stage = if (length(first_stage_results) > 0) do.call(rbind, first_stage_results) else data.frame()
  )
}
