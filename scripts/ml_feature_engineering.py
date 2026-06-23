#!/usr/bin/env python3

"""Shared feature engineering helpers for large-batch CTR models."""

from __future__ import annotations

import math
from typing import Any

TARGET_COLUMN = "label"
DROP_COLUMNS = {"dataset_split", "raw_event_id", "batch_id", "click_flag", "impression_count", "click_count"}
RAW_CATEGORICAL_COLUMNS = ["c1", "c6", "c19", "c20", "c22", "c25", "c26"]
BUCKET_CODE_COLUMNS = [f"i{index}_bucket_code" for index in range(1, 14)]
NUMERIC_BUCKET_LIFT_COLUMNS = [f"i{index}_bucket_ctr_lift" for index in range(1, 14)]
CATEGORICAL_LIFT_COLUMNS = [f"{column}_ctr_lift" for column in RAW_CATEGORICAL_COLUMNS]
CATEGORICAL_SUPPORT_COLUMNS = [f"{column}_feature_impressions" for column in RAW_CATEGORICAL_COLUMNS]
BASE_EXCLUDED_TEXT_COLUMNS = {"event_batch"}
ENCODING_PRIOR = 50.0


def select_source_feature_columns(feature_columns: list[str]) -> list[str]:
    return [
        column
        for column in feature_columns
        if column not in BASE_EXCLUDED_TEXT_COLUMNS
    ]


def _safe_numeric_series(frame, column_name: str):
    import pandas as pd

    if column_name not in frame.columns:
        return pd.Series([0.0] * len(frame), index=frame.index, dtype="float64")
    return pd.to_numeric(frame[column_name], errors="coerce").fillna(0.0)


def _build_encoding_map(train_df, column_name: str, *, global_ctr: float, prior: float) -> dict[str, Any]:
    series = train_df[column_name].fillna("unknown").astype(str)
    targets = train_df[TARGET_COLUMN].astype(int)
    total_rows = max(1, len(train_df))

    value_stats: dict[str, dict[str, float]] = {}
    aggregate: dict[str, list[int]] = {}
    for value, target in zip(series.tolist(), targets.tolist()):
        bucket = aggregate.setdefault(value, [0, 0])
        bucket[0] += 1
        bucket[1] += int(target)

    for value, (count, clicks) in aggregate.items():
        smoothed_ctr = (clicks + (global_ctr * prior)) / (count + prior)
        value_stats[value] = {
            "smoothed_ctr": float(smoothed_ctr),
            "frequency": float(count / total_rows),
            "log_count": float(math.log1p(count)),
        }

    return {
        "default_ctr": float(global_ctr),
        "default_frequency": 0.0,
        "default_log_count": 0.0,
        "values": value_stats,
    }


def build_encoding_bundle(train_df, *, prior: float = ENCODING_PRIOR) -> dict[str, Any]:
    global_ctr = float(train_df[TARGET_COLUMN].astype(int).mean())
    encoded_columns = [column for column in [*RAW_CATEGORICAL_COLUMNS, *BUCKET_CODE_COLUMNS] if column in train_df.columns]
    encodings = {
        column: _build_encoding_map(train_df, column, global_ctr=global_ctr, prior=prior)
        for column in encoded_columns
    }
    return {
        "version": "v3_target_encoding_scaled",
        "prior": float(prior),
        "global_ctr": global_ctr,
        "encoded_columns": encoded_columns,
        "encodings": encodings,
    }


def apply_feature_engineering(frame, *, encoding_bundle: dict[str, Any]):
    import pandas as pd
    feature_map: dict[str, Any] = {}

    # Core numeric and missingness signals.
    for column_name in [
        "event_day_number",
        "missing_numeric_count",
        "missing_categorical_count",
        "has_missing_numeric",
        "has_missing_categorical",
        "high_missingness_flag",
        *[f"i{index}" for index in range(1, 14)],
        *[f"i{index}_log_scale" for index in range(1, 14)],
        *NUMERIC_BUCKET_LIFT_COLUMNS,
        *CATEGORICAL_LIFT_COLUMNS,
        "overall_ctr",
    ]:
        if column_name in frame.columns:
            feature_map[column_name] = _safe_numeric_series(frame, column_name)

    for column_name in CATEGORICAL_SUPPORT_COLUMNS:
        if column_name in frame.columns:
            support_series = _safe_numeric_series(frame, column_name)
            feature_map[f"{column_name}_log1p"] = support_series.map(lambda value: math.log1p(max(value, 0.0)))

    engineered = pd.DataFrame(feature_map, index=frame.index)

    if NUMERIC_BUCKET_LIFT_COLUMNS[0] in frame.columns:
        bucket_lift_frame = pd.DataFrame(
            {column_name: _safe_numeric_series(frame, column_name) for column_name in NUMERIC_BUCKET_LIFT_COLUMNS if column_name in frame.columns}
        )
        engineered["bucket_lift_sum"] = bucket_lift_frame.sum(axis=1)
        engineered["bucket_lift_mean"] = bucket_lift_frame.mean(axis=1)
        engineered["bucket_lift_max"] = bucket_lift_frame.max(axis=1)
        engineered["bucket_lift_positive_count"] = (bucket_lift_frame > 0).sum(axis=1).astype(float)
        engineered["bucket_lift_negative_count"] = (bucket_lift_frame < 0).sum(axis=1).astype(float)

    if CATEGORICAL_LIFT_COLUMNS[0] in frame.columns:
        categorical_lift_frame = pd.DataFrame(
            {column_name: _safe_numeric_series(frame, column_name) for column_name in CATEGORICAL_LIFT_COLUMNS if column_name in frame.columns}
        )
        engineered["categorical_lift_sum"] = categorical_lift_frame.sum(axis=1)
        engineered["categorical_lift_mean"] = categorical_lift_frame.mean(axis=1)
        engineered["categorical_lift_max"] = categorical_lift_frame.max(axis=1)

    support_log_columns = [f"{column_name}_log1p" for column_name in CATEGORICAL_SUPPORT_COLUMNS if f"{column_name}_log1p" in engineered.columns]
    if support_log_columns:
        support_log_frame = engineered[support_log_columns]
        engineered["categorical_support_log_sum"] = support_log_frame.sum(axis=1)
        engineered["categorical_support_log_mean"] = support_log_frame.mean(axis=1)

    log_scale_columns = [f"i{index}_log_scale" for index in range(1, 14) if f"i{index}_log_scale" in engineered.columns]
    if log_scale_columns:
        log_scale_frame = engineered[log_scale_columns]
        engineered["numeric_log_sum"] = log_scale_frame.sum(axis=1)
        engineered["numeric_log_mean"] = log_scale_frame.mean(axis=1)
        engineered["numeric_log_max"] = log_scale_frame.max(axis=1)

    missing_numeric = engineered.get("missing_numeric_count", 0)
    missing_categorical = engineered.get("missing_categorical_count", 0)
    engineered["total_missing_count"] = missing_numeric + missing_categorical
    engineered["missing_ratio"] = engineered["total_missing_count"] / 39.0

    if "bucket_lift_mean" in engineered.columns and "event_day_number" in engineered.columns:
        engineered["event_day_x_bucket_lift_mean"] = engineered["event_day_number"] * engineered["bucket_lift_mean"]
    if "bucket_lift_mean" in engineered.columns and "high_missingness_flag" in engineered.columns:
        engineered["high_missing_x_bucket_lift_mean"] = engineered["high_missingness_flag"] * engineered["bucket_lift_mean"]
    if "categorical_lift_mean" in engineered.columns and "missing_ratio" in engineered.columns:
        engineered["missing_ratio_x_categorical_lift_mean"] = engineered["missing_ratio"] * engineered["categorical_lift_mean"]

    encodings = encoding_bundle.get("encodings", {})
    for column_name in encoding_bundle.get("encoded_columns", []):
        if column_name not in frame.columns:
            continue
        series = frame[column_name].fillna("unknown").astype(str)
        mapping = encodings.get(column_name, {})
        values_map = mapping.get("values", {})
        default_ctr = float(mapping.get("default_ctr", encoding_bundle.get("global_ctr", 0.0)))
        default_frequency = float(mapping.get("default_frequency", 0.0))
        default_log_count = float(mapping.get("default_log_count", 0.0))

        engineered[f"{column_name}_enc_ctr"] = series.map(
            lambda value: float(values_map.get(value, {}).get("smoothed_ctr", default_ctr))
        )
        engineered[f"{column_name}_enc_ctr_lift"] = engineered[f"{column_name}_enc_ctr"] - float(
            encoding_bundle.get("global_ctr", 0.0)
        )
        engineered[f"{column_name}_enc_log_count"] = series.map(
            lambda value: float(values_map.get(value, {}).get("log_count", default_log_count))
        )

    engineered = engineered.fillna(0.0).astype("float32")
    engineered_columns = list(engineered.columns)
    return engineered, engineered_columns


def update_scaler_state(
    scaler_state: dict[str, Any] | None,
    feature_frame,
    *,
    feature_columns: list[str] | None = None,
) -> dict[str, Any]:
    import numpy as np

    if feature_columns is None:
        feature_columns = list(feature_frame.columns)
    matrix = feature_frame[feature_columns].to_numpy(dtype="float64", copy=False)
    chunk_count = matrix.shape[0]
    chunk_sum = matrix.sum(axis=0)
    chunk_sum_sq = np.square(matrix).sum(axis=0)

    if scaler_state is None:
        return {
            "columns": feature_columns,
            "count": int(chunk_count),
            "sum": chunk_sum,
            "sum_sq": chunk_sum_sq,
        }

    scaler_state["count"] += int(chunk_count)
    scaler_state["sum"] += chunk_sum
    scaler_state["sum_sq"] += chunk_sum_sq
    return scaler_state


def finalize_scaler_bundle(scaler_state: dict[str, Any]) -> dict[str, Any]:
    import numpy as np

    count = max(int(scaler_state["count"]), 1)
    means = scaler_state["sum"] / count
    variances = (scaler_state["sum_sq"] / count) - np.square(means)
    variances = np.clip(variances, a_min=1e-8, a_max=None)
    scales = np.sqrt(variances)
    return {
        "columns": list(scaler_state["columns"]),
        "means": means.astype("float32").tolist(),
        "scales": scales.astype("float32").tolist(),
        "version": "standard_score_v1",
    }


def apply_scaler(feature_frame, scaler_bundle: dict[str, Any]):
    import numpy as np
    import pandas as pd

    columns = list(scaler_bundle.get("columns", feature_frame.columns))
    means = np.asarray(scaler_bundle.get("means", []), dtype="float32")
    scales = np.asarray(scaler_bundle.get("scales", []), dtype="float32")
    matrix = feature_frame[columns].to_numpy(dtype="float32", copy=True)
    if len(means) == matrix.shape[1] and len(scales) == matrix.shape[1]:
        matrix = (matrix - means) / scales
    return pd.DataFrame(matrix, columns=columns, index=feature_frame.index, dtype="float32")
