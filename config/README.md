# Config Directory

This folder centralises *all* knobs that control the dummy pipeline.  
Every orchestrator run reads these YAMLs, so edits here propagate automatically through the workflow.

## Files

| File | What it controls |
| --- | --- |
| `globals.yaml` | Absolute/relative paths, execution mode (`dummy` vs `real`), RNG seed, and hardware hints (threads). The orchestrator uses these values to create/clean `data_work/**` directories. |
| `params.yaml` | Analysis grid and dummy-sample tuning. It declares outcomes, flows, partners, base years, periods, IV labels, transform options, and synthetic sample sizes/noise. Tweaking these keys is the sanctioned way to change which regression specs are produced. |
| `schemas.yaml` | Column-level contracts for every artefact the pipeline touches (dimensions, facts, exposures, panels, results). Validator steps in the orchestrator compare outputs to these schemas to catch regressions early. |

## Editing guidance

1. **Prefer additive edits** – extend arrays (e.g., add periods or IVs) rather than modifying existing values unless you intend to regenerate the dummy history.
2. **Keep types stable** – schemas expect consistent types (`int`, `float`, `str`). Make sure new values respect those expectations.
3. **Sync documentation** – when you add new keys or targets, update `README.md` and relevant folder READMEs so downstream users understand the change.
4. **Re-run the pipeline** – any change under `config/` requires rerunning `Rscript scripts/R/orchestrate.R run` to materialise the new configuration.

For large structural changes (new transforms, extra flows), review `scripts/R/lib/` to ensure the estimation logic is aware of the new labels.
