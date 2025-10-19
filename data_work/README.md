# data_work Directory

All pipeline artefacts land here. The folder mirrors the stages of the orchestrator so we can inspect or swap any link in the chain when we move from dummy to production data.

> **Important:** Only documentation files are versioned. Actual CSV/HTML outputs remain local because they are large and reproducible. Do **not** add real data to Git.

## Subdirectories

| Path | Contains | When populated |
| --- | --- | --- |
| `raw/` | Synthetic “raw” extracts (Sakernas micro, HS trade, Susenas micro) broken out by year. | Immediately after running `orchestrate.R` in dummy mode. |
| `interim/` | Lightly cleaned versions of the raw extracts (e.g., harmonised trade, cleaned Sakernas). | Post cleaning steps within the orchestrator. |
| `processed/` | Canonical artefacts used downstream: labor shares, national shocks, tariff shocks, exposures, instruments, panel. | Before estimation. |
| `output/` | Results ready for dissemination—CSV regression grids, first-stage diagnostics, HTML tables, PDFs. | Final step of the pipeline. |
| `logs/` | Run logs and trace files (time stamps, warnings) produced during orchestration. | Throughout the run. |
| `meta/` | Lineage manifests and validation reports (JSON) that describe how artefacts were produced. | Final step of the pipeline. |

Each of these folders ships with its own README (`README.md`) explaining the expected file patterns. See those notes before dropping any files inside.

## Cleaning up

Use the orchestrator’s `clean` command to wipe all generated data safely:

```bash
Rscript scripts/R/orchestrate.R clean
```

This honours the paths in `config/globals.yaml` so you do not accidentally delete hand-curated datasets.

## Switching to real data

When moving beyond the dummy pipeline, keep the same directory structure:

1. Replace dummy generators with real extractors that emit files matching the documented schemas.
2. Store heavy upstream data (micro files, zipped extracts) in cloud storage and download them on demand rather than committing them here.
3. Continue tracking only metadata/README files in Git.
