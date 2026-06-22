# ML Notes

## Current Status

The ML extension has now started at the platform-foundation and training-dataset level.

Completed in this step:
- `ml` schema added to the project
- ML metadata tables created
- ML scoring table created
- initial ML validation SQL added
- setup runner added
- batch-aware ML dataset extraction script added
- split manifest generation added
- ML dataset artifacts now register in ops metadata

This means the project can now store:
- model versions
- training runs
- model evaluation metrics
- batch-level prediction outputs

## Core Files

- `sql/21_ml_schema.sql`
- `sql/23_ml_checks.sql`
- `scripts/ml_setup.py`
- `scripts/ml_training_dataset.py`
- `docs/ml_extension_workflow.md`

## Current ML Objects

### Tables

- `ml.model_registry`
- `ml.training_runs`
- `ml.model_metrics`
- `ml.prediction_scores`

### Views

- `ml.latest_training_metrics`

## Why This Step Matters

This step makes the ML extension part of the platform design instead of a future notebook idea.

It gives us a structured place to store:
- which model was trained
- how it was evaluated
- which batch was scored
- what probabilities were produced

It also gives us a repeatable way to extract:
- train split artifacts
- validation split artifacts
- test split artifacts
- split metadata for later training scripts

## Setup Command

Run from the project root:

```bash
python3 scripts/ml_setup.py
```

## Validation Command

Run the ML foundation checks:

```bash
psql -d ctr_analytics -f sql/23_ml_checks.sql
```

## Next Recommended Step

After the ML schema foundation is in place, the next best step is:

1. build the logistic-regression baseline first
2. store training-run and metric outputs in the new ML tables
3. then add boosted-tree model comparison
