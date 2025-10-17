generate_dimensions <- function(params, seed) {
  set.seed(seed)
  n_districts <- params$dummy_data$n_districts
  n_industries <- params$dummy_data$n_industries
  year_bounds <- params$dummy_data$year_range
  years <- seq.int(year_bounds[[1]], year_bounds[[2]])
  partners <- params$analysis$partners

  dim_time_year <- data.frame(
    year = years,
    era_label = ifelse(years <= 2007, "pre_2008", "post_2008"),
    stringsAsFactors = FALSE
  )

  prov_count <- 34L
  district_ids <- seq_len(n_districts)
  dim_geo_district_2000 <- data.frame(
    district_bps_2000 = district_ids,
    prov_code = sample.int(prov_count, n_districts, replace = TRUE),
    district_name = sprintf("District_%03d", district_ids),
    stringsAsFactors = FALSE
  )

  industry_ids <- seq_len(n_industries)
  dim_sector_isic_rev3 <- data.frame(
    isic_rev3_3d = 100 + industry_ids,
    industry_name = sprintf("ISIC_%02d", industry_ids),
    stringsAsFactors = FALSE
  )

  partner_names <- c(
    CHN = "China",
    USA = "United States",
    JPN = "Japan",
    WLD_minus_IDN = "World minus Indonesia"
  )
  dim_partner <- data.frame(
    iso3 = partners,
    name = partner_names[partners],
    stringsAsFactors = FALSE
  )

  list(
    dim_time_year = dim_time_year,
    dim_geo_district_2000 = dim_geo_district_2000,
    dim_sector_isic_rev3 = dim_sector_isic_rev3,
    dim_partner = dim_partner
  )
}

generate_concordances <- function(dim_sector, seed) {
  set.seed(seed + 11L)
  industries <- dim_sector$isic_rev3_3d
  hs_per_industry <- 6L
  records <- lapply(industries, function(industry) {
    codes <- sprintf("%06d", sample(100000:999999, hs_per_industry))
    allocations <- runif(hs_per_industry, 0.1, 1)
    allocations <- allocations / sum(allocations)
    data.frame(
      hs6 = codes,
      hs_rev = ifelse(runif(hs_per_industry) > 0.5, "HS1996", "HS2007"),
      isic_rev3_3d = industry,
      allocation_share = allocations,
      source = sample(c("synthetic", "legacy_bridge"), hs_per_industry, replace = TRUE),
      stringsAsFactors = FALSE
    )
  })
  do.call(rbind, records)
}

generate_sakernas_raw <- function(dim_geo, dim_sector, years, params, seed) {
  set.seed(seed + 101L)
  district_ids <- dim_geo$district_bps_2000
  industry_ids <- dim_sector$isic_rev3_3d
  n_districts <- length(district_ids)
  n_industries <- length(industry_ids)
  min_year <- min(years)

  workers_target <- params$dummy_data$sakernas_workers_per_dy
  if (is.null(workers_target)) workers_target <- 400

  shares_2000 <- matrix(0, nrow = n_districts, ncol = n_industries)
  shares_2008 <- matrix(0, nrow = n_districts, ncol = n_industries)

  service_bias <- seq(1, 1.6, length.out = n_industries)
  for (d_idx in seq_len(n_districts)) {
    weights2000 <- rgamma(n_industries, shape = runif(1, 2, 8), rate = 1)
    shares_2000[d_idx, ] <- weights2000 / sum(weights2000)

    drift_weights <- shares_2000[d_idx, ] * service_bias + runif(n_industries, 0, 0.05)
    shares_2008[d_idx, ] <- drift_weights / sum(drift_weights)
  }

  raw_records <- vector("list", length = n_districts * length(years))
  idx <- 1L

  for (district in district_ids) {
    d_idx <- match(district, district_ids)
    for (year in years) {
      w <- pmin(pmax((year - 2000) / 8, 0), 1)
      share_mix <- (1 - w) * shares_2000[d_idx, ] + w * shares_2008[d_idx, ]
      share_mix <- share_mix / sum(share_mix)

      growth <- 1 + 0.018 * (year - min_year) + rnorm(1, 0, 0.02)
      n_workers <- max(120L, round(workers_target * growth))

      industries <- sample(industry_ids, size = n_workers, replace = TRUE, prob = share_mix)
      education <- sample(
        c("primary", "secondary", "tertiary"),
        size = n_workers,
        replace = TRUE,
        prob = c(0.42, 0.38, 0.20)
      )
      wage <- rgamma(
        n_workers,
        shape = 10 + match(industries, industry_ids) / 4,
        rate = 0.08
      ) * 100

      worker_ids <- sprintf("W%03d_%d_%04d", district, year, seq_len(n_workers))
      raw_records[[idx]] <- data.frame(
        worker_id = worker_ids,
        district_bps_2000 = district,
        year = year,
        isic_rev3_3d = industries,
        education_level = education,
        wage_monthly = wage,
        stringsAsFactors = FALSE
      )
      idx <- idx + 1L
    }
  }

  do.call(rbind, raw_records)
}

process_sakernas_raw <- function(sakernas_df, dim_geo, dim_sector, params) {
  if (!all(c("district_bps_2000", "isic_rev3_3d", "year") %in% names(sakernas_df))) {
    stop("Sakernas raw data is missing required columns.")
  }

  if ("worker_id" %in% names(sakernas_df)) {
    fact_labor <- aggregate(
      worker_id ~ district_bps_2000 + isic_rev3_3d + year,
      data = sakernas_df,
      length
    )
  } else if ("employment" %in% names(sakernas_df)) {
    fact_labor <- aggregate(
      employment ~ district_bps_2000 + isic_rev3_3d + year,
      data = sakernas_df,
      sum
    )
  } else {
    sakernas_df$employment <- 1
    fact_labor <- aggregate(
      employment ~ district_bps_2000 + isic_rev3_3d + year,
      data = sakernas_df,
      sum
    )
  }
  names(fact_labor)[names(fact_labor) == tail(names(fact_labor), 1)] <- "labor_count"

  district_ids <- dim_geo$district_bps_2000
  industry_ids <- dim_sector$isic_rev3_3d
  share_list <- list()
  base_years <- params$analysis$base_years

  for (base_year in base_years) {
    share_matrix <- matrix(0, nrow = length(district_ids), ncol = length(industry_ids))
    base_subset <- fact_labor[fact_labor$year == base_year, , drop = FALSE]
    if (nrow(base_subset) > 0) {
      totals <- aggregate(
        labor_count ~ district_bps_2000,
        data = base_subset,
        sum
      )
      base_subset <- merge(
        base_subset,
        totals,
        by = "district_bps_2000",
        suffixes = c("", "_total"),
        all.x = TRUE
      )
      base_subset$district_bps_2000 <- as.integer(base_subset$district_bps_2000)
      base_subset$isic_rev3_3d <- as.integer(base_subset$isic_rev3_3d)
      base_subset$labor_count_total[base_subset$labor_count_total <= 0] <- NA_real_
      base_subset$share <- base_subset$labor_count / base_subset$labor_count_total
      valid <- !is.na(base_subset$share)
      d_idx <- match(base_subset$district_bps_2000[valid], district_ids)
      i_idx <- match(base_subset$isic_rev3_3d[valid], industry_ids)
      valid <- valid & !is.na(d_idx) & !is.na(i_idx)
      share_matrix[cbind(d_idx[valid], i_idx[valid])] <- base_subset$share[valid]
    }
    share_list[[as.character(base_year)]] <- share_matrix
  }

  fact_labor <- fact_labor[order(fact_labor$district_bps_2000, fact_labor$isic_rev3_3d, fact_labor$year), ]

  result <- list(
    fact_labor = fact_labor,
    shares = share_list
  )

  for (name in names(share_list)) {
    result[[paste0("shares_", name)]] <- share_list[[name]]
  }

  result
}

generate_trade_raw <- function(dim_sector, partners, flows, years, concordance, seed) {
  set.seed(seed + 137L)
  industry_ids <- dim_sector$isic_rev3_3d
  n_years <- length(years)
  deflator <- exp(0.02 * (years - 2008))

  records <- list()
  idx <- 1L

  for (industry in industry_ids) {
    industry_effect <- rnorm(1, 0, 0.2)
    mapping <- concordance[concordance$isic_rev3_3d == industry, ]
    if (nrow(mapping) == 0) next
    mapping$allocation_share <- mapping$allocation_share / sum(mapping$allocation_share)

    for (partner in partners) {
      partner_effect <- switch(
        partner,
        CHN = 0.35,
        USA = 0.25,
        JPN = 0.15,
        WLD_minus_IDN = 0.2,
        0.1
      )
      partner_shock <- cumsum(rnorm(n_years, 0, 0.05))
      for (flow in flows) {
        flow_effect <- if (flow == "import") 0.4 else 0.22
        cyclical <- sin(seq_len(n_years) / 2 + partner_effect) * 0.08
        trend <- seq(0, 0.28, length.out = n_years)
        noise <- rnorm(n_years, 0, 0.04)
        level <- 11 + industry_effect + partner_effect + flow_effect
        nominal_series <- exp(level + trend + cyclical + partner_shock + noise)

        for (y_idx in seq_len(n_years)) {
          year <- years[y_idx]
          defl <- deflator[y_idx]
          for (m_idx in seq_len(nrow(mapping))) {
            alloc <- mapping$allocation_share[m_idx]
            hs_value <- nominal_series[y_idx] * alloc * exp(rnorm(1, 0, 0.02))
            records[[idx]] <- data.frame(
              year = year,
              hs6 = mapping$hs6[m_idx],
              hs_rev = mapping$hs_rev[m_idx],
              partner_iso3 = partner,
              flow_code = flow,
              trade_value_nominal = hs_value,
              deflator_2008 = defl,
              stringsAsFactors = FALSE
            )
            idx <- idx + 1L
          }
        }
      }
    }
  }

  do.call(rbind, records)
}

process_trade_raw <- function(trade_raw, concordance) {
  merged <- merge(
    trade_raw,
    concordance,
    by = c("hs6", "hs_rev"),
    all.x = TRUE
  )
  merged$allocation_share[is.na(merged$allocation_share)] <- 1

  merged$value_usd_nominal <- merged$trade_value_nominal * merged$allocation_share
  merged$value_usd_real_2008 <- merged$value_usd_nominal / merged$deflator_2008

  fact_trade <- aggregate(
    cbind(value_usd_nominal, value_usd_real_2008) ~ isic_rev3_3d + partner_iso3 + year + flow_code,
    data = merged,
    sum
  )

  fact_trade[order(
    fact_trade$isic_rev3_3d,
    fact_trade$partner_iso3,
    fact_trade$flow_code,
    fact_trade$year
  ), ]
}

compute_national_shocks <- function(fact_trade) {
  fact_trade <- fact_trade[order(
    fact_trade$isic_rev3_3d,
    fact_trade$partner_iso3,
    fact_trade$flow_code,
    fact_trade$year
  ), ]

  grouped <- split(
    fact_trade,
    list(fact_trade$isic_rev3_3d, fact_trade$partner_iso3, fact_trade$flow_code),
    drop = TRUE
  )

  shock_records <- lapply(grouped, function(df) {
    values <- df$value_usd_real_2008
    shift <- c(NA, values[-length(values)])
    shock <- log(values + 1e-6) - log(shift + 1e-6)
    shock[is.na(shock)] <- 0
    data.frame(
      isic_rev3_3d = df$isic_rev3_3d,
      partner_iso3 = df$partner_iso3,
      year = df$year,
      flow_code = df$flow_code,
      shock_log_diff = shock,
      stringsAsFactors = FALSE
    )
  })

  do.call(rbind, shock_records)
}

generate_exposures <- function(shock_df, labor_info, dim_geo, dim_sector, params) {
  years <- sort(unique(shock_df$year))
  base_years <- params$analysis$base_years
  flows <- params$analysis$flows
  partners <- params$analysis$partners

  district_ids <- dim_geo$district_bps_2000
  industry_ids <- dim_sector$isic_rev3_3d
  share_list <- labor_info$shares

  exposures <- vector("list", length = length(base_years) * length(flows) * length(partners))
  idx <- 1L

  for (base_year in base_years) {
    share_key <- as.character(base_year)
    share_matrix <- NULL
    if (!is.null(share_list)) {
      share_matrix <- share_list[[share_key]]
    }
    if (is.null(share_matrix)) {
      share_matrix <- labor_info[[paste0("shares_", share_key)]]
    }
    if (is.null(share_matrix)) {
      stop(sprintf("Missing labor share matrix for base year %s.", base_year))
    }
    for (flow in flows) {
      for (partner in partners) {
        exposure_matrix <- matrix(0, nrow = length(district_ids), ncol = length(years))
        for (ind_idx in seq_along(industry_ids)) {
          industry <- industry_ids[ind_idx]
          shock_subset <- shock_df[
            shock_df$isic_rev3_3d == industry &
              shock_df$partner_iso3 == partner &
              shock_df$flow_code == flow,
            ,
            drop = FALSE
          ]
          shock_subset <- shock_subset[match(years, shock_subset$year), , drop = FALSE]
          shock_vector <- shock_subset$shock_log_diff
          shock_vector[is.na(shock_vector)] <- 0
          exposure_matrix <- exposure_matrix + share_matrix[, ind_idx, drop = FALSE] %*% t(shock_vector)
        }

        records <- expand.grid(
          district_bps_2000 = district_ids,
          year = years,
          stringsAsFactors = FALSE
        )
        records$flow_code <- flow
        records$partner_iso3 <- partner
        records$base_year <- base_year
        records$exposure_log_diff <- as.vector(t(exposure_matrix))

        exposures[[idx]] <- records
        idx <- idx + 1L
      }
    }
  }

  do.call(rbind, exposures)
}

generate_instruments <- function(exposure_df, params) {
  iv_records <- list()
  idx <- 1L

  exposure_key <- function(flow, partner, base_year) {
    subset(
      exposure_df,
      flow_code == flow & partner_iso3 == partner & base_year == base_year,
      select = c("district_bps_2000", "year", "exposure_log_diff")
    )
  }

  partners <- params$analysis$partners
  base_years <- params$analysis$base_years

  for (base_year in base_years) {
    chn_import <- exposure_key("import", "CHN", base_year)
    wld_import <- exposure_key("import", "WLD_minus_IDN", base_year)
    usa_import <- exposure_key("import", "USA", base_year)
    jpn_import <- exposure_key("import", "JPN", base_year)

    if (nrow(chn_import) > 0 && nrow(wld_import) > 0) {
      diff_df <- merge(chn_import, wld_import, by = c("district_bps_2000", "year"), suffixes = c("_chn", "_wld"))
      diff_df$instrument <- diff_df$exposure_log_diff_chn - diff_df$exposure_log_diff_wld
      diff_df$instrument <- lag_vector(diff_df$instrument, 1)
      diff_df$iv_label <- "chn_wld_minus_idn_lag1"
      diff_df$base_year <- base_year
      iv_records[[idx]] <- diff_df[, c("district_bps_2000", "year", "iv_label", "instrument", "base_year")]
      idx <- idx + 1L
    }

    if (nrow(usa_import) > 0) {
      usa_df <- usa_import
      usa_df$instrument <- lag_vector(usa_df$exposure_log_diff, 1)
      usa_df$iv_label <- "usa_lag1"
      usa_df$base_year <- base_year
      iv_records[[idx]] <- usa_df[, c("district_bps_2000", "year", "iv_label", "instrument", "base_year")]
      idx <- idx + 1L
    }

    if (nrow(jpn_import) > 0) {
      jpn_df <- jpn_import
      jpn_df$instrument <- lag_vector(jpn_df$exposure_log_diff, 1)
      jpn_df$iv_label <- "jpn_lag1"
      jpn_df$base_year <- base_year
      iv_records[[idx]] <- jpn_df[, c("district_bps_2000", "year", "iv_label", "instrument", "base_year")]
      idx <- idx + 1L
    }
  }

  instruments <- do.call(rbind, iv_records)
  names(instruments)[names(instruments) == "instrument"] <- "instrument_log_diff_lag1"
  instruments
}

generate_susenas_raw <- function(dim_geo, years, exposures, params, seed) {
  set.seed(seed + 173L)
  district_ids <- dim_geo$district_bps_2000
  min_year <- min(years)

  households_target <- params$dummy_data$susenas_households_per_dy
  if (is.null(households_target)) households_target <- 320

  import_exposure <- exposures[
    exposures$flow_code == "import" &
      exposures$partner_iso3 == "CHN" &
      exposures$base_year == 2000,
    c("district_bps_2000", "year", "exposure_log_diff")
  ]
  names(import_exposure)[names(import_exposure) == "exposure_log_diff"] <- "exp_import"

  export_exposure <- exposures[
    exposures$flow_code == "export" &
      exposures$partner_iso3 == "CHN" &
      exposures$base_year == 2000,
    c("district_bps_2000", "year", "exposure_log_diff")
  ]
  names(export_exposure)[names(export_exposure) == "exposure_log_diff"] <- "exp_export"

  exposure_lookup <- merge(import_exposure, export_exposure, by = c("district_bps_2000", "year"), all = TRUE)
  exposure_lookup$exp_import[is.na(exposure_lookup$exp_import)] <- 0
  exposure_lookup$exp_export[is.na(exposure_lookup$exp_export)] <- 0

  base_income <- rnorm(length(district_ids), mean = 10.4, sd = 0.25)
  names(base_income) <- district_ids

  records <- vector("list", length = length(district_ids) * length(years))
  idx <- 1L

  for (district in district_ids) {
    for (year in years) {
      exp_row <- exposure_lookup[
        exposure_lookup$district_bps_2000 == district &
          exposure_lookup$year == year,
        ,
        drop = FALSE
      ]
      exp_import <- if (nrow(exp_row) == 0) 0 else exp_row$exp_import[1]
      exp_export <- if (nrow(exp_row) == 0) 0 else exp_row$exp_export[1]

      growth <- 1 + 0.02 * (year - min_year) + rnorm(1, 0, 0.015)
      n_households <- max(150L, round(households_target * growth))

      ln_income_base <- base_income[as.character(district)]
      meanlog <- ln_income_base + 0.02 * (year - min_year) - 0.25 * exp_import + 0.1 * exp_export
      incomes <- rlnorm(n_households, meanlog = meanlog, sdlog = 0.35)
      poverty_line <- exp(9.9 + 0.018 * (year - min_year))
      gap <- pmax((poverty_line - incomes) / poverty_line, 0)

      urban_prob <- plogis(-0.45 + 0.55 * exp_export + 0.12 * (year - min_year) / 4)
      urban_prob <- pmin(pmax(urban_prob, 0.1), 0.95)
      urban <- rbinom(n_households, 1, urban_prob)

      hs_prob <- plogis(-0.9 + 0.6 * urban + 0.35 * exp_export)
      hs_prob <- pmin(pmax(hs_prob, 0.05), 0.9)
      hs_flag <- rbinom(n_households, 1, hs_prob)

      literacy_prob <- pmin(pmax(0.7 + 0.18 * urban + 0.04 * (year - min_year), 0.55), 0.99)
      literacy <- rbinom(n_households, 1, literacy_prob)

      household_ids <- sprintf("H%03d_%d_%04d", district, year, seq_len(n_households))
      records[[idx]] <- data.frame(
        household_id = household_ids,
        district_bps_2000 = district,
        year = year,
        income = incomes,
        poverty_line = poverty_line,
        poor_gap = gap,
        poor_gap_sq = gap^2,
        urban = urban,
        hs_or_above = hs_flag,
        literacy = literacy,
        ln_income_base = ln_income_base,
        stringsAsFactors = FALSE
      )
      idx <- idx + 1L
    }
  }

  do.call(rbind, records)
}

process_susenas_raw <- function(susenas_raw) {
  susenas_raw$poor_indicator <- as.integer(susenas_raw$poor_gap > 0)

  agg <- aggregate(
    cbind(
      p0 = susenas_raw$poor_indicator,
      p1 = susenas_raw$poor_gap,
      p2 = susenas_raw$poor_gap_sq,
      urban_share = susenas_raw$urban,
      share_hs_or_above = susenas_raw$hs_or_above,
      literacy_rate = susenas_raw$literacy,
      ln_income_1997_base = susenas_raw$ln_income_base
    ) ~ district_bps_2000 + year,
    data = susenas_raw,
    mean
  )

  agg[order(agg$district_bps_2000, agg$year), ]
}

assemble_panel <- function(exposures, outcomes, instruments, params) {
  panel <- merge(
    exposures,
    outcomes,
    by = c("district_bps_2000", "year"),
    all.x = TRUE,
    all.y = FALSE
  )

  if (!is.null(instruments) && nrow(instruments) > 0) {
    panel <- merge(
      panel,
      instruments,
      by = c("district_bps_2000", "year", "base_year"),
      all.x = TRUE
    )
  } else {
    panel$iv_label <- NA_character_
    panel$instrument_log_diff_lag1 <- NA_real_
  }

  periods <- params$analysis$periods
  for (period_label in periods) {
    flag <- apply_period_flag(panel$year, period_label)
    panel[[paste0("period_", period_label)]] <- as.integer(flag)
  }

  panel
}
