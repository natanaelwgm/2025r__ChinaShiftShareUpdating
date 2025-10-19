# data_work/meta

Metadata and lineage artefacts produced by the orchestrator:

- `*.json` manifests containing input hashes, row counts, and timestamps.
- Validation summaries that complement the numeric diagnostics under `data_work/output/diagnostics/`.

Use these files to audit how a particular run was constructed (e.g., which config revision, how many rows, when executed).  
They are lightweight but reproducible, so feel free to purge them before rerunning the pipeline.
