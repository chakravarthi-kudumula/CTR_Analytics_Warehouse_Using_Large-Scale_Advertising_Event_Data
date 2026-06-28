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
- active-canonical model resolution added to the scoring paths
- a canonical `1M` ML batch was rebuilt and used for the active Logistic Regression result
- model-comparison SQL view added so earlier small-batch experiments can be compared honestly against the canonical `1M` run
- tuned `v4` and calibrated `v4_calibrated` model versions added on top of the canonical `1M` batch

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

### Current best practical Logistic Regression result

The best current practical model is `ctr_logistic_regression v4_calibrated`, built on top of the tuned `v4` large-batch logistic model and validated on the canonical `1M` batch.

Current metrics:

- validation ROC-AUC: `0.751863`
- validation PR-AUC: `0.527533`
- validation log loss: `0.483303`
- validation precision@10%: `0.632787`
- validation lift@10%: `2.520879`
- test ROC-AUC: `0.751757`
- test PR-AUC: `0.536907`
- test log loss: `0.490528`

Current scored output:

- scored rows: `1,000,000`
- average predicted CTR: `0.241120`
- batch actual CTR: `0.256223`
- top decile actual CTR: `0.642220`
- top decile lift vs batch CTR: `2.506488`

For pure ranking strength, `ctr_logistic_regression v4` remains the strongest uncalibrated version:

- validation ROC-AUC: `0.753310`
- validation PR-AUC: `0.533925`
- validation log loss: `0.597277`
- test ROC-AUC: `0.751757`
- test PR-AUC: `0.536907`
- top decile lift vs batch CTR: `2.506488`

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
- `ml.latest_model_feature_importance`
- `ml.latest_feature_group_importance`

These make it easier to answer:
- what is the latest quality level of each model version
- what is the currently active canonical model
- which models are promotion-eligible against the active baseline
- which model ranked best for a scored batch
- which scored batches should be investigated for drift
- which features and feature groups drive the latest trained model

The platform now also supports canonical model promotion:
- `ml.active_canonical_model` shows which version is currently active
- `ml.active_model_monitoring_dashboard` exposes the active model directly for dashboard and reporting consumption
- `ml.model_promotion_audit` records promotion or rejection decisions
- scheduled retraining candidates are promoted only if they beat the active canonical model on configured ROC-AUC, PR-AUC, and lift thresholds

Current active canonical model:
- `ctr_logistic_regression v4_calibrated`

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
- resolve the active canonical model automatically when a scoring version is not supplied
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
