# scripts/R/lib

Library of R modules sourced by `scripts/R/orchestrate.R`. Each file here delivers a cohesive slice of the pipeline:

| File | Responsibility |
| --- | --- |
| `config.R` | Load YAML settings (`globals`, `params`, `schemas`) and expose helper accessors. |
| `dummy_generators.R` | Build synthetic datasets (labor, trade, tariffs, Susenas outcomes), exposures, and flow-aware instruments. |
| `estimation.R` | 2SLS helper (`run_2sls`) plus the spec grid iterator that writes regression results. |
| `utils.R` | Logging, timing, lag/diff utilities, directory helpers, and general-purpose support routines. |
| `validation.R` | Lightweight checks ensuring labour shares sum to one, instruments are populated, and result counts match expectations. |

Rules of thumb when editing or adding modules:

1. **Keep side effects local** – functions should return data frames; orchestrator handles I/O.
2. **Respect schemas** – every exported table must match the structure defined under `config/schemas.yaml`.
3. **Use config helpers** – fetch options from `config.R` rather than hard-coding spec lists.
4. **Document new files** – update this README whenever you add or remove modules so maintainers know where logic lives.
