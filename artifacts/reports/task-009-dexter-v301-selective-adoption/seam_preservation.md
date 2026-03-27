# Seam Preservation Note

## Frozen boundaries kept intact
- `paper_live` mode label unchanged
- `DexLab/instrumentation.py` unchanged
- replay export path unchanged
- NDJSON event contract unchanged
- no strategy, threshold, or operator-control logic adoption

## Additive behavior only
- New tables live under `phase2_*` and do not replace existing `mints` / `stagnant_mints`.
- Research capture is best effort and cannot block the existing collector/market update path.
- Timestamp normalization is applied only to the new research capture path and snapshot metadata, not to the core trading/runtime decisions.
