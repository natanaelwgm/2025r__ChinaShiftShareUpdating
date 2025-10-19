# R Pipeline Sources

This directory houses the complete R implementation of the dummy shift–share pipeline.  
All steps described in the top-level README are orchestrated from here.

## Entry points

| File | Role |
| --- | --- |
| `orchestrate.R` | Command-line orchestrator (`run`, `clean`, `list`). Loads config YAMLs, executes the pipeline, and persists artefacts. |
| `export_tables_batch.R` | (Optional) Example of producing grouped tables directly from R using `xtable` or `fixest::etable`. |
| `run_etable.R` | CLI wrapper around `fixest::etable()` that respects the transform metadata in the results CSVs. |

## Library folders

The orchestrator sources files under `scripts/R/lib/` (not shown in this README) which define all business logic:

- `config.R` – YAML loader helpers and accessor utilities.
- `dummy_generators.R` – Creates synthetic Sakernas, trade flows, tariffs, Susenas outcomes, exposures, and IVs.
- `estimation.R` – 2SLS helper (`run_2sls`) used across the spec grid.
- `validation.R` – Sanity checks (instrument coverage, share sums).
- `utils.R` – Logging, timing, directory helpers, lag/diff utilities.

Refer to the inline comments in those files for implementation specifics.

## Running the pipeline

```bash
Rscript scripts/R/orchestrate.R run    # build dummy data → regression results
Rscript scripts/R/orchestrate.R clean  # wipe mode-specific folders defined in config/globals.yaml
Rscript scripts/R/orchestrate.R list   # inspect ordered execution steps
```

Key outputs:

- `data_work/processed/dummy/panel_analysis.csv`
- `data_work/output/dummy/results/main_{p0,p1,p2}.csv`
- `data_work/output/dummy/tables/p0-p1-p2_batch.html`
- `data_work/output/dummy/diagnostics/validation_report.json`

## Coding standards

1. **Respect schemas** – any data frame emitted by the helpers should match `config/schemas.yaml`.
2. **Add logging** – use `log_info()` around expensive/critical sections so pipeline runs are traceable.
3. **One source of truth** – read config values via the helpers in `config.R`; never hard-code lists of outcomes, flows, or periods.
4. **No side effects outside `data_work/`** – generated artefacts must remain under the configured paths so they can be cleaned safely.

When adding new functionality, update this README and `config/schemas.yaml` to keep the documentation aligned with the code.
