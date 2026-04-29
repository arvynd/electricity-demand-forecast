# TODO - models.py

## Completed

- [x] Widen `pipeline` type hint in `chronos_forecast` from `Chronos2Pipeline` to `BaseChronosPipeline`
- [x] Fix mutable default argument in `chronos_forecast` — replaced `quantile_levels=[]` with `None` sentinel
- [x] Fix `select_chronos_params` default `device_map` — now defaults to `"cpu"` with CUDA availability check

---

# TODO - ingest_ng_storage.py

## Changes to Add Later

- [ ] Add retry logic with exponential backoff for API requests
- [ ] Add configurable date range via CLI arguments or config
- [ ] Add support for different EIA API endpoints (not just natural gas storage)
- [ ] Add data validation before persisting (check for required columns)
- [ ] Add incremental/delta ingestion mode (only fetch new data since last run)
- [ ] Add compression for JSON/CSV output (gzip)
- [ ] Add progress bar for pagination
- [ ] Add environment variable validation at module load time
- [ ] Add configurable output formats (add ORC support)
- [ ] Add logging configuration file support
- [ ] Add unit tests for `get_all_data` function
- [ ] Add integration tests for main pipeline
