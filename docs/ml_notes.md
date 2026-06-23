# ML Notes

## Current Status

The ML extension has now moved beyond a small demo batch and into a real large-batch workflow.

Completed in this step:
- `ml` schema added to the project
- ML metadata tables created
- ML scoring table created
- initial ML validation SQL added
- setup runner added
- batch-aware ML dataset extraction script added
- split manifest generation added
- ML dataset artifacts now register in ops metadata
- baseline Logistic Regression training script added
- baseline model metrics and model artifacts now have a storage path
- batch scoring script added
- prediction score artifacts now register in ops metadata
- ML monitoring views added for decile performance and score drift
- chunked large-batch training added through `train_ctr_sgd.py`
- chunked large-batch scoring added through `score_ctr_batch_chunked.py`
- a canonical `1M` ML batch was rebuilt and used for the active Logistic Regression result
- model-comparison SQL view added so earlier small-batch experiments can be compared honestly against the canonical `1M` run

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
- `scripts/train_ctr_baseline.py`
- `scripts/score_ctr_batch.py`
- `scripts/train_ctr_sgd.py`
- `scripts/train_ctr_xgboost.py`
- `scripts/score_ctr_batch_chunked.py`
- `sql/22_ml_monitoring_views.sql`
- `sql/24_ml_model_comparison_summary.sql`
- `docs/ml_extension_workflow.md`

## Current ML Objects

### Tables

- `ml.model_registry`
- `ml.training_runs`
- `ml.model_metrics`
- `ml.prediction_scores`

### Views

- `ml.latest_training_metrics`
- `ml.score_decile_performance`
- `ml.top_decile_performance`
- `ml.score_drift_summary`
- `ml.model_comparison_summary`

## Canonical ML Run

The active ML baseline is now based on:

- batch: `criteo_1m_ml_canonical_batch`
- batch id: `11`
- feature rows: `1,000,000`
- train rows: `714,286`
- validation rows: `142,857`
- test rows: `142,857`

The earlier `120`-row and other tiny incoming-batch experiments still exist in metadata for comparison, but they are no longer the main ML story.

### Current canonical Logistic Regression result

The canonical `ctr_logistic_regression v3` result is now backed by the `1M` batch through a chunked SGD-based logistic training path with shared feature engineering and train-derived scaling.

Current metrics:

- validation ROC-AUC: `0.710239`
- validation PR-AUC: `0.398863`
- validation log loss: `6.521392`
- validation precision@10%: `0.435111`
- validation lift@10%: `1.695083`

Current scored output:

- scored rows: `1,000,000`
- average predicted CTR: `0.511890`
- batch actual CTR: `0.256223`
- top decile actual CTR: `0.561030`
- top decile lift vs batch CTR: `2.189616`

### How to interpret earlier small-batch experiments

The earlier tiny incoming-batch experiments are still useful for:
- proving the first ML metadata flow worked
- validating scoring and monitoring objects quickly
- showing model experimentation history in `ml.model_registry`

They are **not** the canonical benchmark for the project anymore.

Use `ml.model_comparison_summary` to distinguish:
- `small_scale` prototype runs
- the canonical `1M` logistic run
- later experimental model runs such as `ctr_xgboost`

New dashboard-ready ML monitoring views now expose:
- `ml.latest_model_monitoring_dashboard`
- `ml.batch_model_rankings`
- `ml.model_drift_watchlist`

These make it easier to answer:
- what is the latest quality level of each model version
- which model ranked best for a scored batch
- which scored batches should be investigated for drift

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

And it now gives us a baseline training path that can:
- train a Logistic Regression CTR model on smaller batches
- switch to a scalable chunked logistic path for larger batches
- evaluate train / validation / test splits
- register model metadata in `ml.model_registry`
- register metric rows in `ml.model_metrics`
- store the model artifact and metric summary under `data/ml/models/`

And it now gives us a scoring path that can:
- load a registered trained model
- score a chosen batch from `feature_store.ctr_training_features`
- write probabilities into `ml.prediction_scores`
- assign score deciles and top-decile flags
- store scoring artifacts under `data/ml/scoring/`
- switch to chunked scoring for million-row batches

And it now gives us a monitoring path that can:
- inspect prediction quality by score decile
- summarize top-decile lift by batch
- compare score behavior batch over batch
- compare old small-batch experiments with the new canonical `1M` run
- support future monitoring dashboards and ML drift checks

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

After the canonical `1M` training and scoring path are in place, the next best step is:

1. improve model quality on the `1M` batch with better large-scale feature engineering
2. compare the scalable logistic baseline against a truly scalable tree-based implementation
3. add richer ML drift thresholds and alert-style interpretation on top of the monitoring views
