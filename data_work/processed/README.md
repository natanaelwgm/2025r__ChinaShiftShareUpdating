# data_work/processed

Canonical artefacts that downstream steps depend on.  
Each CSV here should conform to the schemas defined in `config/schemas.yaml`.

Key outputs (all reproducible):

- `dim_*` – Dimension tables (years, districts, industries, partners).
- `fact_*` – Harmonised fact tables (labor shares, trade shocks, tariff shocks, poverty outcomes).
- `exposure_district_year.csv` – District×year exposure matrix with all transforms.
- `instrument_district_year.csv` – Flow-aware IV series keyed by `(district, year, base_year, flow_code, iv_label)`.
- `panel_analysis.csv` – Final analysis panel fed into the estimator.

When swapping to real data, edit the upstream loader scripts to emit identically structured tables before they land here.  
Never hand-edit the CSVs directly—rerun the orchestrator to keep provenance correct.
