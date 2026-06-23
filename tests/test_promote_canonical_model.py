from __future__ import annotations

import sys
import types
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from promote_canonical_model import evaluate_promotion


class PromoteCanonicalModelTests(unittest.TestCase):
    def test_promotes_when_no_active_model_exists(self) -> None:
        args = types.SimpleNamespace(
            min_roc_auc_improvement=0.005,
            min_pr_auc_improvement=0.005,
            min_lift_improvement=0.05,
        )
        candidate = {
            "rows_trained": 714286,
            "validation_roc_auc": 0.71,
            "validation_pr_auc": 0.39,
            "validation_lift_at_10pct": 1.69,
            "training_run_id": 8,
        }
        promoted, reason = evaluate_promotion(candidate, None, None, args)
        self.assertTrue(promoted)
        self.assertIn("No active canonical model exists", reason)

    def test_rejects_when_candidate_does_not_clear_thresholds(self) -> None:
        args = types.SimpleNamespace(
            min_roc_auc_improvement=0.005,
            min_pr_auc_improvement=0.005,
            min_lift_improvement=0.05,
        )
        candidate = {
            "rows_trained": 714286,
            "validation_roc_auc": 0.71,
            "validation_pr_auc": 0.39,
            "validation_lift_at_10pct": 1.69,
            "training_run_id": 9,
        }
        active = {
            "validation_roc_auc": 0.710239,
            "validation_pr_auc": 0.398863,
            "validation_lift_at_10pct": 2.189616,
            "training_run_id": 8,
        }
        promoted, reason = evaluate_promotion(candidate, active, None, args)
        self.assertFalse(promoted)
        self.assertIn("ROC-AUC threshold not met", reason)
        self.assertIn("PR-AUC threshold not met", reason)
        self.assertIn("lift threshold not met", reason)

    def test_prefers_base_version_over_equal_scheduled_clone(self) -> None:
        args = types.SimpleNamespace(
            min_roc_auc_improvement=0.005,
            min_pr_auc_improvement=0.005,
            min_lift_improvement=0.05,
        )
        candidate = {
            "model_version": "v3",
            "rows_trained": 714286,
            "validation_roc_auc": 0.710239,
            "validation_pr_auc": 0.398863,
            "validation_lift_at_10pct": 1.695083,
            "training_run_id": 8,
        }
        active = {
            "model_version": "v3_20260614",
            "validation_roc_auc": 0.710239,
            "validation_pr_auc": 0.398863,
            "validation_lift_at_10pct": 1.695083,
            "training_run_id": 9,
        }
        promoted, reason = evaluate_promotion(candidate, active, None, args)
        self.assertTrue(promoted)
        self.assertIn("reclaims the canonical base version slot", reason)

    def test_rejects_scheduled_clone_when_base_version_is_not_worse(self) -> None:
        args = types.SimpleNamespace(
            min_roc_auc_improvement=0.005,
            min_pr_auc_improvement=0.005,
            min_lift_improvement=0.05,
        )
        candidate = {
            "model_version": "v3_20260614",
            "rows_trained": 714286,
            "validation_roc_auc": 0.710239,
            "validation_pr_auc": 0.398863,
            "validation_lift_at_10pct": 1.695083,
            "training_run_id": 9,
        }
        base_reference = {
            "model_version": "v3",
            "rows_trained": 714286,
            "validation_roc_auc": 0.710239,
            "validation_pr_auc": 0.398863,
            "validation_lift_at_10pct": 1.695083,
            "training_run_id": 8,
        }
        promoted, reason = evaluate_promotion(candidate, None, base_reference, args)
        self.assertFalse(promoted)
        self.assertIn("does not beat the canonical base version", reason)


if __name__ == "__main__":
    unittest.main()
