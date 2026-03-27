# Handoff

## Decision
- Merge-ready selective adoption.
- Adopt only the observability/research surfaces that remain additive against the pinned Dexter seam.

## What changed
- Added a minimal phase-2 research store for collector raw events and mint lifecycle snapshots.
- Wired the collector and market layer to publish:
  - raw event fingerprints
  - slot-linked raw logs
  - active mint snapshots
  - stagnant mint snapshots
- Preserved the existing replay export path and `paper_live` execution semantics.

## Seam preservation
- `Dexter.py` unchanged.
- `DexLab/instrumentation.py` unchanged.
- `tests/test_paper_live.py` unchanged.
- `tests/test_vexter_instrumentation.py` unchanged.
- `tests/test_wslogs.py` kept the creation-payload mint detection fix and extended coverage around observability capture only.
- The added research layer fails open, so bounded runtime behavior remains on the original collector + market path.

## Validation
- Dexter worktree: `pytest -q` -> `12 passed`
- Clean Vexter validation worktree: `python3.12 -m pytest -q` -> `290 passed`

## Notes
- The original Vexter root worktree contains unrelated duplicate/untracked files, so seam validation was executed from a clean detached worktree at `origin/main`.
- Deferred follow-up candidates remain intentionally out of scope for this task:
  - leaderboard generation capture
  - session/order/fill journals
  - config/operator/runtime snapshot adoption
