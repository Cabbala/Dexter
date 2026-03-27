# Status

- task: `task-009-dexter-v301-selective-adoption`
- lane: `infra / dexter-compat`
- baseline_pin: `ddeb18c0dd21fa3a15d4a6a85573428f7d7ae938`
- upstream_target: `1a31624906114150b4f05a0f8103af0ad383ebe7`
- result: `merge_ready`
- decision: `selective_adoption_only`
- adopted_surface: `collector_raw_events + mint_lifecycle_snapshots`
- seam_status: `preserved`
- runtime_status: `unchanged`
- control_plane_status: `unchanged`
- tests:
  - `pytest -q` -> `12 passed`
  - `python3.12 -m pytest -q` -> `290 passed`
