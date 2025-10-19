# data_work/interim

Holds *intermediate* artefacts produced after light cleaning of the raw extracts.

Typical files (all ignored by Git):

- `dummy_clean_sakernas_year_YYYY.csv` – Cleaned Sakernas counts by district × industry × year.
- `dummy_clean_trade_isic_year_YYYY.csv` – Trade flows aggregated to ISIC level with allocation weights applied.
- `dummy_clean_susenas_year_YYYY.csv` – Household aggregates ready for outcome construction.

These files are deterministic and can be regenerated from scratch by running the orchestrator.  
Avoid hand-editing anything here—apply transformations upstream (raw) or downstream (processed) instead.
