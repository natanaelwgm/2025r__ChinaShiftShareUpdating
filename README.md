# 2025r__ChinaShiftShareUpdating

Repository for rebuilding the China Shift–Share (Bartik) analysis stack with a reproducible, results‑first approach.


## 2025‑10‑17 — Results‑First Dummy Pipeline Plan

We will implement a dependency‑aware pipeline that produces fully calculated regression tables (FE/IV specs) from a single command, using end‑to‑end dummy datasets that mirror real schemas and transformations. Later, we will swap the dummy inputs for real sources without changing downstream code.


### Goal

- Generate regression tables and diagnostics from a one‑shot orchestrator run.
- All artifacts (dims, facts, exposures, panels, estimates) are computed; only the input data are dummy.
- Schema‑ and config‑first so we can later replace dummy generation with real data loaders.


### Strategy

- Results‑first: start at the tables and work backwards to the required panels and upstream facts.
- Schema‑driven: every artifact has a declared schema and validation.
- Deterministic randomness: dummy generators are seeded to create plausible, reproducible data with known relationships (IV relevance, FE variation, Rotemberg weight properties).


### Repository Skeleton

- `config/`
  - `globals.yaml` — Paths, seeds, CPU threads; Python/R toggles.
  - `params.yaml` — Analysis grid (base years, flows, partners, periods, IVs, transforms, sample sizes).
  - `schemas.yaml` — Column‑level contracts for each artifact (dims, facts, panel, results).
- `data_work/`
  - `interim/` — Normalized dummy inputs (HS flows, micro summaries).
  - `processed/` — Harmonized facts (labor shares, shocks, exposures, panel).
  - `output/` — Tables, diagnostics, figures; machine‑readable metadata.
  - `logs/`, `meta/` — Run logs and lineage manifests.
- `scripts/python/`
  - `orchestrator.py` — DAG runner: `run|clean|graph|list`.
  - `lib/` — `dims.py`, `concordance.py`, `dummy_gen.py`, `harmonize.py`, `exposure.py`, `instruments.py`, `panel.py`, `estimate.py`, `export.py`, `diagnostics.py`, `utils.py`.
- `scripts/R/` (parity, optional)
  - `orchestrate.R` — R‑native driver mirroring Python flow.
- `docs/`
  - `TABLE_SPEC.md` — Human mapping for published tables → artifacts/filters.
  - `PIPELINE.md` — DAG overview, artifacts, and schemas.


### Config Defaults (params.yaml)

- Sizes: `n_districts: 288`, `n_industries: 20`, `years: [2000..2015]`
- Base years and periods: `base_years: [2000, 2008]`; `periods: [2000_2007, 2008_2012, 2000_2015]`
- Flows/partners: `flows: [import, export]`; `partners: [CHN, USA, JPN, WLD_minus_IDN]`
- IV sets: `iv_sets: [chn_wld_minus_idn_lag1, usa_lag1, jpn_lag1]`
- Transforms: `exposure_transform: log_diff`; `iv_transform: log_diff_lag1`
- Estimation: `cluster: district_bps_2000`; `fe: [district, year]`; `robust: true`
- Seeds: `seed: 20251017`; `noise_scale: 0.2`


### Data Contracts (Schemas)

- Dimensions
  - `dim_time_year`: `year:int`, `era_label:str`
  - `dim_geo_district_2000`: `district_bps_2000:int`, `prov_code:int`, `district_name:str`
  - `dim_sector_isic_rev3`: `isic_rev3_3d:int`, `industry_name:str`
  - `dim_partner`: `iso3:str`, `name:str`
- Concordances (dummy, versioned)
  - `map_hs_rev_to_rev`: `hs6_old`, `hs_rev_old`, `hs6_new`, `hs_rev_new`, `allocation_share`
  - `map_hs_to_isic`: `hs6`, `hs_rev`, `isic_rev3_3d`, `allocation_share`, `source`
  - `map_admin_to_2000`: `year`, `bps_current`, `district_bps_2000`, `method`, `share_weight`
- Facts (processed)
  - `fact_labor_district_isic_year`: `district_bps_2000`, `isic_rev3_3d`, `year`, `labor_count`, `employment_share_base2000`, `employment_share_base2008`
  - `fact_trade_isic_partner_year_flow`: `isic_rev3_3d`, `partner_iso3`, `year`, `flow_code`, `value_usd_nominal`, `value_usd_real_2008`
  - `fact_poverty_district_year`: `district_bps_2000`, `year`, `p0`, `p1`, `p2`, `urban_share`, `share_hs_or_above`, `literacy_rate`, `ln_income_1997_base`
  - `fact_national_shock_isic_year`: `isic_rev3_3d`, `year`, `flow_code`, `partner_iso3`, `shock_log_diff`
- Exposures & IVs
  - `exposure_district_year`: `district_bps_2000`, `year`, `flow_code`, `partner_iso3`, `base_year`, `exposure_log_diff` (LOO), plus meta
  - `instrument_district_year`: `district_bps_2000`, `year`, `iv_label`, `instrument_log_diff_lag1`
- Panel
  - `panel_analysis`: `district_bps_2000`, `year`, `y (p0/p1/p2)`, `exposure`, controls, and scenario flags
- Results
  - `result_main`: `table_id`, `outcome`, `flow`, `partner`, `base_year`, `period`, `coef`, `se`, `N`, `clusters`, `fe_flags`, `iv_label`, diagnostics
  - `result_first_stage`: `outcome_x`, `instrument`, `F_partial`, `R2_first`, `N`


### DAG (Results‑Backwards)

1) Results targets (root): `main_p0`, `main_p1`, `main_p2`; diagnostics (`first_stage`, `rotemberg`, `pretrends`, `summaries`).
2) Estimation bundle: OLS + IV(2SLS) with FE and clustered SE; exports CSV/HTML and JSON metadata.
3) Panel assembly: join exposures with outcomes/controls; filter periods; tag variants.
4) Exposures: compute LOO share‑weighted exposures; transforms (log, diff, log_diff, lag1).
5) Instruments: build instrument series (e.g., `chn_wld_minus_idn_log_diff_lag1`).
6) National shocks: aggregate trade to isic×year; compute log differences and lags.
7) Harmonized trade: HS→ISIC mapping with allocation; deflate to real USD.
8) Labor shares: district×isic base‑year shares (2000/2008), sum‑to‑1; totals for per‑worker if needed.
9) Outcomes/controls: dummy SUSENAS aggregates (p0/p1/p2, urban, literacy, HS+).
10) Dims & concordances: minimal, versioned, with QA coverage.
11) Lineage & validation: meta manifests for each artifact; validation reports in `logs/` and `output/diagnostics/`.


### Dummy Data Design (Plausible, Reproducible)

- Districts/industries: 288 districts, 20 industries, 2000–2015; provinces and Java flags for heterogeneity.
- Labor shares: base‑2000 via Dirichlet per district; base‑2008 drift toward services; totals provide per‑worker denominators.
- National trade shocks: CHN import surges post‑2001/2009; exports milder; WLD−IDN lower variance; USA/JPN partially correlated for IV relevance.
- HS→ISIC mapping: many‑to‑one mapping with allocation weights; coverage ~1.
- Outcomes/controls: P0 trending down; P1/P2 co‑move; urban/literacy/HS+ trending up; small structural relationships to exposures.
- IV structure: instruments correlated 0.5–0.7 with exposure; partial F ~ 20–40; exclusion holds by construction.
- Rotemberg: weights sum to 1; a few industries dominate identification.


### Estimation Engine (Calculated)

- OLS with FE: within‑transform or FE dummies; robust/clustered SE.
- IV (2SLS): first stage X~Z+FE+controls; second stage Y~Xhat+FE+controls; cluster at district; partial F, Cragg–Donald, Hansen J when applicable.
- Grids: outcomes (p0/p1/p2) × flows (import/export) × base (2000/2008) × IVs × periods (2000–2007, 2008–2012, 2000–2015) × optional heterogeneity.
- Outputs: `output/results/main_<outcome>.csv`, `output/results/first_stage.csv`, diagnostics (Rotemberg, validation), pretty tables.


### Orchestrator Design (Python)

- CLI: `python scripts/python/orchestrator.py run|clean|graph|list [--only target]`.
- Task registry: each task declares `inputs`, `outputs`, `func`, `params`; staleness via mtime + config hash.
- R parity: `scripts/R/orchestrate.R` mirrors steps via `targets`‑like plan; both parse YAML config.


### Validation & QA

- Structural: shares sum‑to‑1; mapping coverage≈1; no duplicate keys; non‑negative values.
- Statistical: IV partial F>10; exposure variance; FE variation.
- Integrity: `.meta.json` for parents/input hashes/row/col counts per artifact.
- Reproducibility: seeded generators yield identical outputs unless params change.


### Extensibility

- Swap dummy→real by replacing dummy loaders with readers that emit the same `interim/` contracts; leave harmonize/exposure/estimate/export untouched.
- Add partners/base years/categories via `params.yaml`; add table variants via `docs/TABLE_SPEC.md` and `export.py` registry.


### Deliverables (First Implementation Pass)

- Configs: `config/globals.yaml`, `config/params.yaml`, `config/schemas.yaml`.
- Orchestrator: Python engine (+ optional R driver stub).
- Implemented modules (dummy end‑to‑end): dims/concordances; dummy labor/trade/outcomes; harmonization (HS→ISIC, deflation); national shocks + instruments; labor shares (2000/2008); exposures with LOO; panel assembly; OLS + IV with FE/clustered SE; exporters and diagnostics.
- Run produces: main tables for p0/p1/p2 across flow/base/period; first‑stage metrics; Rotemberg weights; validation report.


### Proposed Build Order

1) Scaffold repo structure + configs
2) Orchestrator skeleton + task registry
3) Dims + concordances (dummy)
4) Dummy generators (labor, trade HS, SUSENAS outcomes)
5) Harmonization (HS→ISIC, deflation) + national shocks
6) Labor shares + exposures (LOO) + instruments
7) Panel assembly
8) Estimation (OLS + IV) + diagnostics
9) Exporters + table mapping
10) Validation + lineage writers
11) Dry‑run end‑to‑end; tune dummy distributions for healthy IV stats
12) Optional R parity


### Next Steps

- Implement the scaffold and minimal orchestrator with dummy pipeline stubs.
- Target command: `python scripts/python/orchestrator.py run` to emit results in `data_work/output/`.
- After sign‑off, proceed to coding the scaffold and the first dummy end‑to‑end run.

