from __future__ import annotations

import argparse
import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import run_pipeline


class RunPipelineTests(unittest.TestCase):
    def build_args(self, **overrides) -> argparse.Namespace:
        defaults = {
            "sample": "1m",
            "source_path": None,
            "batch_name": None,
            "sample_scale": "incoming",
            "use_spark": True,
            "skip_quality": False,
            "skip_benchmarks": False,
            "host": None,
            "port": "5432",
            "database": "ctr_analytics",
            "maintenance_database": "postgres",
            "user": "postgres",
            "password": "postgres",
        }
        defaults.update(overrides)
        return argparse.Namespace(**defaults)

    def test_determine_batch_name_for_sample(self) -> None:
        args = self.build_args(sample="5m")
        self.assertEqual(run_pipeline.determine_batch_name(args), "criteo_5m_runner_batch")

    def test_build_step_plan_includes_spark_quality_and_benchmarks(self) -> None:
        args = self.build_args()
        plan = run_pipeline.build_step_plan(args)
        labels = [label for label, _ in plan]
        self.assertEqual(
            labels,
            ["raw", "spark", "staging", "warehouse", "marts", "advanced_sql", "feature_store", "quality", "benchmarks"],
        )

    def test_build_step_plan_for_incoming_source_uses_explicit_batch_name(self) -> None:
        args = self.build_args(
            sample=None,
            source_path="/tmp/incoming.csv",
            batch_name="incoming_batch_001",
            use_spark=False,
            skip_quality=True,
            skip_benchmarks=True,
        )
        plan = run_pipeline.build_step_plan(args)
        raw_command = plan[0][1]

        self.assertIn("--source-path", raw_command)
        self.assertIn("/tmp/incoming.csv", raw_command)
        self.assertIn("incoming_batch_001", raw_command)
        self.assertEqual([label for label, _ in plan], ["raw", "staging", "warehouse", "marts", "advanced_sql", "feature_store"])


if __name__ == "__main__":
    unittest.main()
