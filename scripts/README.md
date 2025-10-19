# Scripts Directory

Top-level entry point for every executable component in this project.  
Currently the pipeline runs entirely through R, with a small Python helper for HTML exports.

## Layout

| Path | Description |
| --- | --- |
| `R/` | R sources that power the orchestrator and supporting libraries. See `scripts/R/README.md` for step-by-step details. |
| `export_tables_batch.py` | Python utility that groups the regression results CSVs into an HTML “spec booklet” (all flows × partners × IVs × outcomes). Requires `pandas` installed. |
| `R/export_tables_batch.R` *(generated via etable runner)* | Produces etables or LaTeX/PDF tables when helpful (documented in the R README). |

## Usage summary

| Task | Command |
| --- | --- |
| Run the full pipeline | `Rscript scripts/R/orchestrate.R run` |
| Clean generated artefacts | `Rscript scripts/R/orchestrate.R clean` |
| Export HTML tables | `python scripts/export_tables_batch.py --mode dummy --num 9` |
| Produce per-flow etables | `Rscript scripts/R/run_etable.R --mode dummy --outcome p0 --flow import --partner CHN` |

## Adding new modules

1. **Keep documentation close** – any new subdirectory should ship with a `README.md` explaining its contract.
2. **Expose CLI entry points** – follow the existing pattern (small `optparse` / `argparse` wrapper) so commands are self-documenting.
3. **Respect the config** – read from `config/params.yaml` rather than hard-coding spec lists, IV names, or cluster columns.
4. **Guard dependencies** – prefer `requireNamespace()` / `importlib` checks that fail fast instead of installing packages automatically.

The `scripts` tree is the right place for orchestration, exporters, or diagnostics that transform generated data—not for storing data itself.
