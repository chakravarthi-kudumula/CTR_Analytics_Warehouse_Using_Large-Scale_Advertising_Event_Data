# ML Model Monitoring Overview

This page is meant to sit beside the pipeline operations page and answer three questions quickly:

1. Which model version is currently strongest?
2. How is the latest scored batch performing in top-decile ranking terms?
3. Which model or batch needs drift investigation?

## Main Sources

- `ml.latest_model_monitoring_dashboard`
- `ml.batch_model_rankings`
- `ml.model_drift_watchlist`
- `ml.latest_model_feature_importance`
- `ml.latest_feature_group_importance`

## KPI Cards

- `Best Validation ROC-AUC`
- `Best Validation PR-AUC`
- `Best Top-Decile Lift`
- `Canonical Model Version`
- `Canonical Validation Quality Band`
- `Canonical Ranking Quality Band`

## Recommended Visuals

1. Model Health Table
   - source: `ml.latest_model_monitoring_dashboard`
   - fields:
     - `model_name`
     - `model_version`
     - `train_batch_name`
     - `is_active_canonical`
     - `validation_roc_auc`
     - `validation_pr_auc`
     - `validation_log_loss`
     - `top_decile_lift_vs_batch_ctr`
     - `validation_quality_band`
     - `ranking_quality_band`
     - `drift_severity`

2. Batch Model Ranking Matrix
   - source: `ml.batch_model_rankings`
   - rows:
     - `batch_name`
     - `model_name`
     - `model_version`
   - values:
     - `batch_rank_by_lift`
     - `batch_rank_by_auc`
     - `top_decile_lift_vs_batch_ctr`
     - `validation_roc_auc`

3. Drift Watchlist Table
   - source: `ml.model_drift_watchlist`
   - fields:
     - `batch_name`
     - `model_name`
     - `model_version`
     - `previous_batch_name`
     - `drift_status`
     - `drift_note`
     - `top_decile_lift_delta`
     - `actual_ctr_delta`

4. Top Feature Importance Bar Chart
   - source: `ml.latest_model_feature_importance`
   - axis:
     - `feature_name`
   - values:
     - `abs_importance_value`
   - filters:
     - top `15` by `importance_rank`
   - tooltip:
     - `importance_direction`
     - `interpretation_note`

5. Feature Group Importance Bar Chart
   - source: `ml.latest_feature_group_importance`
   - axis:
     - `feature_group`
   - values:
     - `group_relative_importance_pct`

## Page Layout

- top row:
  - KPI cards
- middle row:
  - model health table
  - batch ranking matrix
- bottom row:
  - drift watchlist
  - top feature importance
  - feature group importance

## Interpretation Notes

- `validation_quality_band = strong` should generally align with `validation_roc_auc >= 0.70`
- `ranking_quality_band = strong` should generally align with `top_decile_lift_vs_batch_ctr >= 2.0`
- `drift_status = investigate` should be visually emphasized
- for linear models, `importance_value` is coefficient-based, while `abs_importance_value` is better for ranking
- positive coefficients mean higher feature values push predicted CTR upward
- negative coefficients mean higher feature values push predicted CTR downward
