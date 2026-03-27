# Task 009: Dexter v3.0.1 Selective Adoption

## Scope
- Source repo: `Cabbala/Dexter`
- Fork seam baseline: `ddeb18c0dd21fa3a15d4a6a85573428f7d7ae938`
- Upstream audit target: `FLOCK4H/Dexter` tag `3.0.1` at `1a31624906114150b4f05a0f8103af0ad383ebe7`
- Adoption mode: selective P1 only

## Adopted P1 surfaces
- `DexLab/wsLogs.py`
  - Preserves raw websocket log lines alongside parsed payloads.
  - Skips undecoded program-data payloads instead of pushing opaque blobs into downstream storage.
  - Records a best-effort raw-event fingerprint before handing events to market storage.
  - Passes `slot` plus the raw-event fingerprint into downstream mint/swap updates.
- `DexLab/market.py`
  - Adds best-effort phase-2 research capture without changing the primary `mints` / `stagnant_mints` flow.
  - Records raw event metadata for collector-observed mint and swap traffic.
  - Records mint lifecycle snapshots for active and stagnant states.
  - Normalizes event timestamps before research capture so replay/research tables stay stable across second/ms/us/ns payload variants.
  - Fails open if phase-2 schema bootstrap or research writes fail.
- `database.py`
  - Creates the additional phase-2 research tables during database initialization.
- `dexter_time.py`
  - Direct selective adoption of the upstream timestamp-normalization helpers.
- `dexter_phase2.py`
  - Minimal compatibility transplant of the upstream phase-2 design, limited to:
    - raw event storage
    - mint registry storage
    - mint snapshot storage
    - stable event fingerprinting
- `dexter_data_store.py`
  - Thin compatibility export for the adopted phase-2 store and schema statements.

## Intentionally excluded
- `Dexter.py`
- `dexter_cli.py`
- `dexter_config.py`
- `dexter_operator.py`
- `dexter_alerts.py`
- `dexter_mev.py`
- `dexter_strategy.py`
- `DexAI/trust_factor.py`
- `settings.py`
- `blacklist.txt`
- Any change to `DexLab/instrumentation.py`
- Any change to `paper_live`, NDJSON, replay export, watchdog, or planner/control-plane contracts

## Why this is safe
- The new research capture is additive only.
- Primary runtime tables and replay exports remain the source of truth.
- The new phase-2 writes are best effort and fail-open.
- No runtime entry/exit logic, strategy thresholds, or operator/control-plane ownership changed.
