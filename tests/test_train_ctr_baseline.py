from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import train_ctr_baseline


class TrainCtrBaselineTests(unittest.TestCase):
    def test_resolve_dataset_name(self) -> None:
        self.assertEqual(
            train_ctr_baseline.resolve_dataset_name("sample_batch", None),
            "ml_training_dataset_sample_batch",
        )
        self.assertEqual(
            train_ctr_baseline.resolve_dataset_name("sample_batch", "custom_dataset"),
            "custom_dataset",
        )

    def test_split_feature_types(self) -> None:
        frame = {
            "i1": [1, 2],
            "i1_log_scale": [0.1, 0.2],
            "c1": ["a", "b"],
            "event_batch": ["1m", "1m"],
            "has_missing_numeric": [1, 0],
        }
        numeric_columns, categorical_columns = train_ctr_baseline.split_feature_types(
            frame,
            ["i1", "i1_log_scale", "c1", "event_batch", "has_missing_numeric"],
        )
        self.assertEqual(numeric_columns, ["i1", "i1_log_scale", "has_missing_numeric"])
        self.assertEqual(categorical_columns, ["c1", "event_batch"])

    def test_compute_ranking_metrics(self) -> None:
        y_true = [1, 0, 1, 0, 0, 1, 0, 0, 0, 0]
        y_score = [0.99, 0.40, 0.95, 0.20, 0.10, 0.80, 0.05, 0.02, 0.01, 0.00]
        precision_at_top, lift_at_top = train_ctr_baseline.compute_ranking_metrics(y_true, y_score)

        self.assertAlmostEqual(precision_at_top, 1.0)
        self.assertGreater(lift_at_top, 1.0)


if __name__ == "__main__":
    unittest.main()
