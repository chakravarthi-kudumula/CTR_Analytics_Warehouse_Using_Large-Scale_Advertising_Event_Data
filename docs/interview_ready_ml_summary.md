# Interview-Ready ML Summary

## What This ML Layer Does

The ML extension turns the CTR analytics platform into a batch-aware click-probability prediction system. It trains models from `feature_store.ctr_training_features`, scores full impression batches into `ml.prediction_scores`, tracks experiments in `ml.model_registry`, stores evaluation metrics in `ml.model_metrics`, and monitors ranking quality, drift, feature importance, and canonical model promotion.

## Main Problem Statement

Build a production-style CTR prediction and impression ranking workflow that estimates click probability for large-scale advertising events, supports offline batch scoring, and keeps the best model version under governance through promotion rules and monitoring.

## Best Current Model

Best practical model:
- `ctr_logistic_regression v4_calibrated`

Why this version matters:
- it preserves the strong ranking performance of the tuned `v4` baseline
- it improves probability realism through calibration
- it fits cleanly into the batch-scoring and promotion framework

## Quantifiable Results

Canonical training batch:
- batch: `criteo_1m_ml_canonical_batch`
- feature rows: `1,000,000`
- train rows: `714,286`
- validation rows: `142,857`
- test rows: `142,857`

`v3` baseline:
- validation ROC-AUC: `0.710239`
- validation PR-AUC: `0.398863`
- validation log loss: `6.521392`
- top-decile lift on scored batch: `2.189616`

`v4` tuned model:
- validation ROC-AUC: `0.753310`
- validation PR-AUC: `0.533925`
- validation log loss: `0.597277`
- test ROC-AUC: `0.751757`
- test PR-AUC: `0.536907`
- top-decile lift on scored batch: `2.506488`

`v4_calibrated` practical winner:
- validation ROC-AUC: `0.751863`
- validation PR-AUC: `0.527533`
- validation log loss: `0.483303`
- test ROC-AUC: `0.751757`
- test PR-AUC: `0.536907`
- test log loss: `0.490528`
- top-decile lift on scored batch: `2.506488`

Probability-quality improvement:
- `v3` average predicted CTR: `0.511890`
- `v4` average predicted CTR: `0.436209`
- `v4_calibrated` average predicted CTR: `0.241120`
- actual batch CTR: `0.256223`

This means the tuned-and-calibrated model kept the stronger ranking quality while moving predicted CTR much closer to the observed click rate.

## Why This Is Strong

This is not a notebook-only model experiment. It is a platform-integrated ML workflow with:
- batch-aware dataset extraction
- large-scale training on a `1M` canonical batch
- full-batch scoring back into PostgreSQL
- calibration
- feature importance extraction
- promotion logic
- active-canonical model filtering
- Airflow ML orchestration

## If Someone Asks “Why Should I Care?”

This project shows more than model training. It shows that the model was built inside a real data platform, improved through measurable tuning, scored at scale on a `1M` batch, calibrated for probability quality, and governed through promotion and monitoring rules. It demonstrates both ML engineering and data-platform thinking, not just experimentation.
