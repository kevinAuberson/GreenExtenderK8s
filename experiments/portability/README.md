# portability

Offline replay of the carbon-aware temporal shifting engine against
historical grid carbon-intensity traces from zones other than
Switzerland, to check whether the mechanism's behaviour generalises
beyond the CH grid it was designed and tuned on (report section 4.2.3).

`temporal.py` and `workload_classifier.py` are imported **unchanged**
from `extender/` (via `sys.path`, no code duplication). `scoring.py` is
intentionally **not** exercised: since alpha and CI_norm cancel in node
scoring for a single-site cluster, node placement isn't expected to
change between zones — only the decision to delay a pod is. Node power
(watts) is held constant across time since no per-zone hardware
telemetry exists; CO2 is therefore proportional to CI(t), and every
reduction percentage reported is intensity-weighted and independent of
that constant.

Zones cover a deliberate mix of grid profiles: DE, ES, PL for Europe,
and three US regions with different generation mixes — see
`TRACE_FILES` in `portability_simulation.py` for the rationale behind
each pick.

## Contents

- `traces/` — one Electricity Maps historical CSV export per zone.
- `portability_simulation.py` — runs the replay for every trace found.
- `make_portability_charts.py` — turns the results into the figures used
  in report section 6.4.
- `results/portability_results.json` — raw output of the last run.

## Running the simulation

1. **Drop trace files** into `traces/`, one CSV per zone, named
   `<ZONE>_<YEAR>.csv` (e.g. `DE_2026.csv`) in the Electricity Maps
   historical export format. Any zone present in `traces/` is picked up
   automatically — no code change needed to add one.

2. **Run the replay**:

   ```bash
   python experiments/portability/portability_simulation.py
   ```

   Writes `results/portability_results.json`. If no CSV files are found
   in `traces/`, it prints a message and exits without writing anything.

3. **Generate the charts** (optional, needs matplotlib/numpy):

   ```bash
   pip install matplotlib numpy
   python experiments/portability/make_portability_charts.py
   ```

   Reads `results/portability_results.json` and writes `fig_portability_*.png`
   into `results/`. Reads whatever zones/classes are present in the file,
   so re-running after adding a zone needs no script edits.
