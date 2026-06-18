# Feature Store Notes

## Purpose

This stage builds an ML-ready feature store on top of the warehouse, marts, and advanced analytics layers.

## Main Output

- `feature_store.ctr_training_features`

## Design Choice

The feature store is designed to stay honest to the Criteo dataset.

That means it does not invent business entities like campaign, browser, device, or user. Instead, it combines:

- raw numeric features
- selected categorical features
- missingness indicators
- derived numeric bucket codes
- bucket-level CTR lift features
- categorical feature CTR lift features
- lightweight log-scaled numeric transforms

## Why This Is Useful

This gives the project a real bridge into future CTR modeling workflows such as:

- logistic regression
- XGBoost
- LightGBM

without needing to rebuild feature engineering outside the warehouse.

## Current Coverage

The current feature store includes:

- `label`
- `event_day_number`
- `event_batch`
- `missing_numeric_count`
- `missing_categorical_count`
- `has_missing_numeric`
- `has_missing_categorical`
- `high_missingness_flag`
- raw numeric features `I1` to `I13`
- log-scaled numeric features
- selected categorical features:
  - `C1`
  - `C6`
  - `C19`
  - `C20`
  - `C22`
  - `C25`
  - `C26`
- numeric bucket code features
- numeric bucket CTR lift features
- selected categorical CTR lift features
- selected categorical feature impression counts

## Validation

Use:

- `sql/18_feature_store_checks.sql`

## Build

Run from the project root:

`python3 scripts/feature_store_build.py`
