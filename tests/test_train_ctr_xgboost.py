from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import train_ctr_xgboost


class TrainCtrXgboostTests(unittest.TestCase):
    def test_compute_scale_pos_weight_balanced_case(self) -> None:
        self.assertAlmostEqual(train_ctr_xgboost.compute_scale_pos_weight([1, 0, 0, 1, 0, 0]), 2.0)

    def test_compute_scale_pos_weight_handles_single_class(self) -> None:
        self.assertEqual(train_ctr_xgboost.compute_scale_pos_weight([0, 0, 0]), 1.0)
        self.assertEqual(train_ctr_xgboost.compute_scale_pos_weight([1, 1, 1]), 1.0)


if __name__ == "__main__":
    unittest.main()
