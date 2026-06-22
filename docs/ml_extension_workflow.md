# ML Extension Workflow

## Purpose

This document defines the recommended machine-learning extension for the CTR analytics platform so we can implement it step by step without losing the production-style shape of the project.

The goal is **not** to add a basic notebook classifier.

The goal is to extend the platform into a:

- CTR prediction system
- impression ranking system
- batch scoring workflow
- ML monitoring workflow

## Recommended ML Problem Statement

Build a batch-aware CTR prediction and impression ranking system that uses warehouse-generated features to estimate click probability for large-scale advertising events, support offline model evaluation, and produce batch scoring outputs for downstream analytics and optimization.

## Why This Is The Right ML Direction

This dataset is strongest for:
- click probability prediction
- sparse feature engineering
- high-cardinality feature modeling
- impression ranking
- offline batch scoring
- score drift and calibration monitoring

This dataset is **not** especially strong for:
- revenue modeling
- conversion prediction
- attribution
- user-journey modeling
- causal optimization
- campaign-entity modeling with business-friendly dimensions

So the ML extension should stay focused on CTR prediction and ranking.

## Final ML Recommendation

### Main objective

Predict the probability of click for each ad impression.

### Business-style objective

Rank impressions by click likelihood so downstream systems can prioritize higher-propensity impressions.

### Engineering-style objective

Use the existing feature store, batch metadata, quality framework, Airflow pipeline, and drift layer to support repeatable model training, batch scoring, and monitoring.

## ML Architecture Fit

The existing platform already gives us the right foundation:

```text
incoming file / sampled batch
-> raw
-> staging
-> warehouse
-> marts
-> feature_store.ctr_training_features
-> ML training
-> ML scoring
-> ML monitoring
```

Recommended extension:

```text
feature_store.ctr_training_features
-> training dataset build
-> model training and evaluation
-> model registry metadata
-> batch scoring output
-> monitoring views for score drift, lift, and calibration
```

## What We Should Build

### 1. ML schema

Recommended schema:
- `ml`

Recommended objects:
- `ml.model_registry`
- `ml.training_runs`
- `ml.prediction_scores`
- `ml.model_metrics`
- `ml.score_drift_summary`
- `ml.top_decile_performance`

### 2. Core scoring table

Recommended main output:
- `ml.prediction_scores`

Suggested columns:
- `prediction_id`
- `raw_event_id`
- `batch_id`
- `model_name`
- `model_version`
- `training_run_id`
- `predicted_ctr`
- `score_decile`
- `is_top_decile`
- `scored_at`

Why this matters:
- gives the ML extension a real platform output
- makes the project more than training-only
- lets us measure score quality by batch later

### 3. Model metadata tables

#### `ml.model_registry`

Suggested columns:
- `model_id`
- `model_name`
- `model_version`
- `model_type`
- `feature_source`
- `training_start_date`
- `training_end_date`
- `notes`
- `registered_at`

#### `ml.training_runs`

Suggested columns:
- `training_run_id`
- `model_id`
- `train_batch_name`
- `validation_batch_name`
- `rows_trained`
- `rows_validated`
- `run_status`
- `started_at`
- `completed_at`

#### `ml.model_metrics`

Suggested columns:
- `training_run_id`
- `dataset_split`
- `roc_auc`
- `pr_auc`
- `log_loss`
- `brier_score`
- `precision_at_10pct`
- `lift_at_10pct`
- `recorded_at`

## Model Strategy

## Phase M1 - baseline

Train:
- Logistic Regression

Why:
- strong interpretable baseline
- easy to explain in interviews
- useful benchmark against tree-based methods

## Phase M2 - production-style stronger models

Train:
- LightGBM
- XGBoost

Why:
- better fit for sparse and mixed feature sets
- more realistic CTR modeling progression
- strong performance comparison story

## Phase M3 - optional additional comparison

Optional:
- CatBoost

Only add this if we want one more model family. It is not required for a strong project.

## Evaluation Strategy

Do **not** use accuracy as the main success metric.

CTR is an imbalanced problem, so use:
- `ROC-AUC`
- `PR-AUC`
- `log loss`
- `Brier score`
- calibration curve analysis
- `precision@top-k`
- lift by score decile

### Best business-facing evaluation question

Can the model identify a small top-ranked group of impressions with much higher click behavior than the overall average?

That is a much stronger framing than plain binary classification accuracy.

## Validation Strategy

Because the dataset does not have a true event timestamp, do not pretend we have exact chronological validation.

Recommended validation choices:
- split by batch
- split by derived `event_day_number`
- train on earlier day buckets and validate on later buckets
- compare performance across sampled scales where useful

Suggested initial approach:
- train: event day buckets `1-5`
- validate: event day bucket `6`
- test: event day bucket `7`

Alternative:
- train on `1m`
- validate on a held-out portion or later day buckets
- score a separate incoming/demo batch

Current canonical implementation note:
- the active large-batch baseline uses a chunked SGD-based logistic training path under the canonical model name `ctr_logistic_regression`
- small-batch experiments are still retained in metadata, but they are treated as prototype runs rather than the main benchmark story

## Feature Strategy

Start from:
- `feature_store.ctr_training_features`

Current feature groups already available:
- raw numeric features
- log-scaled numeric features
- selected categorical features
- numeric bucket codes
- missingness features
- CTR-lift signals
- selected feature impression counts

Recommended feature additions:
- categorical frequency counts for selected features
- interaction features for top-performing feature pairs
- batch-level historical support counts
- top-decile segment membership flags
- drift-aware stability indicators later if needed

## Strong ML Positioning

This should be positioned as:
- a feature-store-driven ML extension
- a batch scoring extension
- a ranking-oriented advertising model

It should **not** be positioned as:
- a generic scikit-learn notebook
- a random binary classification exercise
- a deep-learning showcase for hype

## End-To-End ML Workflow

## Step 1 - create ML schema and metadata tables

Build:
- `ml.model_registry`
- `ml.training_runs`
- `ml.model_metrics`
- `ml.prediction_scores`

Output of this step:
- the platform is ready to store model metadata and scores

## Step 2 - create training dataset script

Create a script to extract features from:
- `feature_store.ctr_training_features`

Suggested script name:
- `scripts/ml_training_dataset.py`

Responsibilities:
- pull feature rows from Postgres
- define train/validation/test split
- write local training-ready artifact if needed
- record row counts

Output of this step:
- repeatable training dataset extraction

## Step 3 - build baseline model training script

Suggested script name:
- `scripts/train_ctr_baseline.py`

Responsibilities:
- load training dataset
- train Logistic Regression
- evaluate on validation/test split
- write metrics to `ml.model_metrics`
- register model in `ml.model_registry`
- write training run metadata to `ml.training_runs`

Output of this step:
- first production-style baseline model

## Step 4 - build advanced model training script

Suggested script name:
- `scripts/train_ctr_boosted_models.py`

Responsibilities:
- train LightGBM and/or XGBoost
- compare against baseline
- record metrics consistently
- choose champion model by log loss + ranking metrics

Output of this step:
- stronger production-grade CTR models with comparison evidence

## Step 5 - build batch scoring script

Suggested script name:
- `scripts/score_ctr_batch.py`

Responsibilities:
- load champion model
- score one batch of feature-store rows
- write predictions into `ml.prediction_scores`
- compute `score_decile`
- mark top-decile rows

Output of this step:
- real scoring layer inside the platform

## Step 6 - build ML monitoring views

Suggested SQL file:
- `sql/21_ml_monitoring_views.sql`

Recommended views:
- `ml.score_drift_summary`
- `ml.top_decile_performance`
- `ml.calibration_summary`

Metrics to include:
- average predicted CTR by batch
- score distribution drift by batch
- actual CTR in top score deciles
- lift in top decile vs overall batch CTR
- calibration gap by score bucket

Output of this step:
- model monitoring integrated with the existing ops/quality mindset

## Step 7 - optional Airflow integration

Suggested Airflow extension tasks:
- build training dataset
- train baseline model
- score selected batch
- refresh ML monitoring views

Do this only after the manual/local ML flow works.

Output of this step:
- ML branch becomes orchestrated, not just script-driven

## Suggested File Additions

Recommended files to create later:
- `sql/21_ml_schema.sql`
- `sql/22_ml_monitoring_views.sql`
- `sql/23_ml_checks.sql`
- `scripts/ml_training_dataset.py`
- `scripts/train_ctr_baseline.py`
- `scripts/train_ctr_boosted_models.py`
- `scripts/score_ctr_batch.py`
- `docs/ml_notes.md`

## Suggested Table Build Order

1. create ML schema and metadata tables
2. extract training dataset from feature store
3. train logistic regression baseline
4. train boosted-tree models
5. store metrics and register model versions
6. score a batch into `ml.prediction_scores`
7. build monitoring views on top of the scores
8. optionally orchestrate with Airflow

## Recommended Success Criteria

We should consider the ML extension strong when all of these are true:
- at least one baseline model and one stronger model are implemented
- evaluation uses ranking/probability metrics, not just accuracy
- model metrics are stored in database tables
- batch scoring writes to `ml.prediction_scores`
- score drift and top-decile performance are queryable
- the workflow is documented and repeatable

## What Makes This ML Add-On Strong

This becomes a strong ML extension because it is:
- tied to a real platform
- feature-store driven
- batch-aware
- ranking-oriented
- monitoring-aware
- measurable across batches

That is much stronger than simply saying:
- “I trained a model to predict clicks.”

## Final Recommendation

If we build ML on top of this project, we should build:

**a production-grade CTR prediction and impression ranking extension with batch scoring, model metrics, and monitoring**

and we should implement it in the order defined in this document.
