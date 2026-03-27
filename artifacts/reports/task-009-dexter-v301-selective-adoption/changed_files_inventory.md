# Changed Files Inventory

- `DexLab/market.py`
  - Added best-effort phase-2 raw-event and mint-snapshot capture.
- `DexLab/wsLogs.py`
  - Added raw log propagation, fingerprint capture, and undecoded-payload filtering.
- `database.py`
  - Added phase-2 schema creation on database initialization.
- `dexter_time.py`
  - Added timestamp normalization helpers adopted from upstream v3.0.1.
- `dexter_phase2.py`
  - Added minimal phase-2 schema and store implementation.
- `dexter_data_store.py`
  - Added thin export wrapper for the adopted phase-2 store.
- `tests/test_wslogs.py`
  - Added observability-path regression coverage.
- `tests/test_phase2_observability.py`
  - Added phase-2 normalization and fail-open coverage.
