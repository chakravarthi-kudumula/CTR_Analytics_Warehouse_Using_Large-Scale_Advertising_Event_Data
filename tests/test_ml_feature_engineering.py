from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from ml_feature_engineering import (
    apply_feature_engineering,
    apply_scaler,
    build_encoding_bundle,
    finalize_scaler_bundle,
    select_source_feature_columns,
    update_scaler_state,
)


class MlFeatureEngineeringTests(unittest.TestCase):
    def test_select_source_feature_columns_keeps_bucket_codes(self) -> None:
        selected = select_source_feature_columns(["event_batch", "c1", "i1_bucket_code", "i1_log_scale"])
        self.assertEqual(selected, ["c1", "i1_bucket_code", "i1_log_scale"])

    def test_apply_feature_engineering_builds_encoded_and_aggregate_features(self) -> None:
        try:
            import pandas as pd
        except ModuleNotFoundError:
            self.skipTest("pandas is not installed in this local test environment")

        train_df = pd.DataFrame(
            {
                "label": [1, 0, 1],
                "c1": ["a", "a", "b"],
                "i1_bucket_code": ["low", "high", "low"],
                "missing_numeric_count": [1, 0, 2],
                "missing_categorical_count": [0, 1, 1],
                "high_missingness_flag": [0, 0, 1],
                "event_day_number": [1, 2, 3],
                "i1_log_scale": [0.1, 0.2, 0.3],
                "i1_bucket_ctr_lift": [0.05, -0.02, 0.03],
                "c1_ctr_lift": [0.02, 0.02, -0.01],
                "c1_feature_impressions": [10, 10, 5],
                "overall_ctr": [0.2, 0.2, 0.2],
            }
        )
        bundle = build_encoding_bundle(train_df)
        engineered, feature_columns = apply_feature_engineering(train_df.drop(columns=["label"]), encoding_bundle=bundle)

        self.assertIn("c1_enc_ctr", engineered.columns)
        self.assertIn("i1_bucket_code_enc_ctr_lift", engineered.columns)
        self.assertIn("bucket_lift_sum", engineered.columns)
        self.assertIn("categorical_support_log_sum", engineered.columns)
        self.assertIn("missing_ratio", engineered.columns)
        self.assertEqual(feature_columns, list(engineered.columns))
        self.assertFalse(engineered.isnull().any().any())

        scaler_state = update_scaler_state(None, engineered, feature_columns=feature_columns)
        scaler_bundle = finalize_scaler_bundle(scaler_state)
        scaled = apply_scaler(engineered, scaler_bundle)
        self.assertEqual(list(scaled.columns), feature_columns)
        self.assertEqual(len(scaler_bundle["columns"]), len(feature_columns))


if __name__ == "__main__":
    unittest.main()
