# data_work/logs

Run-time logs and trace files. Typical contents:

- `orchestrator.log` – Step-by-step execution logs from `orchestrate.R`.
- `*.stderr` / `*.stdout` – Captured command outputs (if redirected).
- Timestamped subfolders when multiple runs are executed on the same day.

These files are useful for debugging but are not meant for version control.  
Delete them freely—new runs will generate fresh logs.
