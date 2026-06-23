from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from extract_model_feature_importance import classify_feature_group


class ExtractModelFeatureImportanceTests(unittest.TestCase):
    def test_classify_feature_group(self) -> None:
        self.assertEqual(classify_feature_group("missing_ratio"), "missingness")
        self.assertEqual(classify_feature_group("i1_log_scale"), "numeric_log")
        self.assertEqual(classify_feature_group("i3_bucket_code_enc_ctr"), "bucket_signal")
        self.assertEqual(classify_feature_group("c1_enc_ctr"), "target_encoding")
        self.assertEqual(classify_feature_group("event_day_x_bucket_lift_mean"), "bucket_signal")


if __name__ == "__main__":
    unittest.main()
