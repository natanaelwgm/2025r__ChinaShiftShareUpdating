# China Shift‑Share (2025 refresh)

End‑to‑end rebuild of the “China Shock” (import‐exposure) specification grid with a deterministic dummy pipeline.  
Running `scripts/R/orchestrate.R` produces the complete stack of artifacts—dimensions, labor shares, national shocks, district‑level exposures, IVs, panels, regressions, and publication‑ready tables—so we can swap to real data later without touching downstream code.

---

## Quick start

```bash
# 1. Install required R packages once
Rscript -e "install.packages(c('yaml','fixest','xtable'), repos='https://cloud.r-project.org')"

# 2. Build the pipeline (dummy data → tables)
Rscript scripts/R/orchestrate.R run

# 3. Review outputs
#   data_work/output/dummy/results/main_p0.csv
#   data_work/output/dummy/tables/p0-p1-p2_batch.html
```

Optional utilities:

```bash
# Regenerate HTML “spec booklet” for all flows/IVs/outcomes
python scripts/export_tables_batch.py --mode dummy --num 9

# Produce LaTeX/HTML etables for a specific flow/partner/outcome
Rscript scripts/R/run_etable.R --mode dummy --outcome p0 --flow import --partner CHN --num 6
```

---

## Repository layout

| Path | Purpose |
| --- | --- |
| `README.md` | This guide—project intent, execution commands, and data policies. |
| `config/` | Central YAMLs: `globals.yaml` (paths, seeds), `params.yaml` (analysis grid & dummy sizes), `schemas.yaml` (column contracts). |
| `scripts/` | Orchestration & export code. Currently we operate through the R stack (`scripts/R/**`) plus a Python HTML exporter. |
| `data_work/` | Staging area for generated artifacts. The subfolders are ignored in Git except for documentation READMEs. |

See the per‑directory README files for drill‑downs on expectations and interfaces.

---

## What the R pipeline does

The orchestrator (`scripts/R/orchestrate.R`) executes the following steps in order:

1. **Dims & concordances** – districts, industries, partners, year labels, HS↔ISIC mapping.
2. **Dummy generators** – synthetic Sakernas (labor), HS trade flows, Susenas outcomes/controls.
3. **Trade harmonisation** – HS→ISIC aggregation, GDP deflation, and national log‑diff shocks for each flow/partner.
4. **Tariff shocks** – share‑weighted tariff series to support Kiskatos/Sparrow‑style IVs.
5. **Labor shares** – district×industry weights for base years 2000 & 2008.
6. **Exposures** – district×year matrices for import/export/tariff with level/log/diff & lagged transforms (12 combinations).
7. **Instruments** – flow‑aware IVs (China–ROW, USA, Japan) keyed by `(flow_code, base_year, iv_label)`.
8. **Panel assembly** – join exposures, IVs, and Susenas outcomes/controls, and stamp period flags.
9. **Estimation** – `fixest::feols` 2SLS with district+year FE, district clustering, partial‑F diagnostics.
10. **Exports & diagnostics** – `main_*.csv`, `first_stage.csv`, validation JSON, Rotemberg stats, HTML tables.

All dummy random draws are seeded (`config/globals.yaml: execution.seed`) to keep the outputs perfectly reproducible.

---

## Key configuration knobs (`config/params.yaml`)

| Field | Description |
| --- | --- |
| `analysis.outcomes` | Outcome columns estimated (`p0`, `p1`, `p2`). |
| `analysis.flows` | Flows exposed in both exposures and IVs (`import`, `export`, `tariff`). |
| `analysis.partners` | Trading partners used when generating shocks/IVs (`CHN`, `USA`, `JPN`, `WLD_minus_IDN`). |
| `analysis.base_years` | Labor share base years (2000, 2008). |
| `analysis.periods` | Period windows (short/boom/full). |
| `analysis.iv_sets` | Instrument labels exposed downstream. |
| `analysis.estimation` | Fixed‑effect structure, cluster dimension, and robust flag used by `fixest`. |
| `dummy_data.*` | Sample sizes and noise tuning for synthetic Sakernas/Susenas/trade draws. |

Modifying these parameters changes pipeline behaviour immediately on the next run; schemas guard against missing columns or type drift.

---

## Generated data (`data_work/`)

The pipeline writes into six subdirectories—`raw`, `interim`, `processed`, `output`, `logs`, `meta`.  
Each directory contains a README describing its intended contents and the reason the actual data are excluded from Git.  
Only the documentation files are versioned; CSVs, HTML tables, and logs stay local to keep the repository light.

If you need to re‑run from a clean slate, use:

```bash
Rscript scripts/R/orchestrate.R clean   # wipes mode-specific folders defined in config/globals.yaml
```

---

## Exporting tables

- **HTML booklet** – `python scripts/export_tables_batch.py --mode dummy --num 9` groups specs by `(flow, partner, iv)` and lists all 12 exposure/outcome transforms per block (P0/P1/P2).
- **fixest etable** – `scripts/R/run_etable.R` respects the transform metadata and config cluster column, producing LaTeX/HTML tables per flow/partner/outcome trio.
- **Spec book (R)** – `scripts/R/export_specbook.R` renders a PDF spec booklet that mirrors the results CSV, including metadata such as IV name and cluster level.

All exporters assume the pipeline has already generated the corresponding CSV results.

---

## Data management & Git policy

- `data_work/**` is ignored except for README documents; please keep real data in external storage (S3/GCS/etc.) or reproduce via the orchestrator.
- Synthetic draws are deterministic. When we switch to production data, replace the dummy generators with real loaders that emit the same schemas.
- Large binary artifacts should never be committed. If you need to cache them, use an object store or Git LFS in a separate pipeline.

---

## Branch housekeeping

- `main` – current, working branch (this repository).
- `main_old` – snapshot of the pre-refresh history kept for reference.
- Promote changes by committing on `main` and pushing (force-with-lease if schema shifts require it). Use the README files to document any new directories you create.

---

## Support & next steps

- Replace dummy readers with production data sources while keeping schemas untouched.
- Extend transforms/IVs by editing `config/params.yaml` and adding table metadata.
- Harden the Python orchestrator parity (`scripts/python/…`) if/when needed; the `scripts/README.md` documents expectations for future modules.

For questions, open an issue or ping the maintainers listed in commit history. Happy shifting! 🚀
