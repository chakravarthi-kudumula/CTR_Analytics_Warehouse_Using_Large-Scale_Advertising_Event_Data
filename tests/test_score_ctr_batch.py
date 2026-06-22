from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import score_ctr_batch


class ScoreCtrBatchTests(unittest.TestCase):
    def test_assign_score_deciles_preserves_top_rank_as_decile_ten(self) -> None:
        scores = [0.95, 0.80, 0.60, 0.20, 0.10]
        deciles = score_ctr_batch.assign_score_deciles(scores)

        self.assertEqual(deciles[0], 10)
        self.assertGreater(deciles[1], deciles[3])
        self.assertEqual(len(deciles), len(scores))

    def test_assign_score_deciles_spreads_full_ten_bands(self) -> None:
        scores = list(range(20, 0, -1))
        deciles = score_ctr_batch.assign_score_deciles(scores)

        self.assertEqual(sorted(set(deciles)), list(range(1, 11)))
        self.assertEqual(deciles[0], 10)
        self.assertEqual(deciles[-1], 1)

    def test_build_score_summary(self) -> None:
        class FakeFrame:
            empty = False

            def __init__(self):
                self.rows = [
                    {"predicted_ctr": 0.9, "actual_click": 1, "is_top_decile": True},
                    {"predicted_ctr": 0.8, "actual_click": 1, "is_top_decile": True},
                    {"predicted_ctr": 0.3, "actual_click": 0, "is_top_decile": False},
                    {"predicted_ctr": 0.1, "actual_click": 0, "is_top_decile": False},
                ]

            def __getitem__(self, key):
                if isinstance(key, str):
                    return FakeSeries([row[key] for row in self.rows])
                return FakeFrameFiltered([row for row, keep in zip(self.rows, key) if keep])

            def __len__(self):
                return len(self.rows)

        class FakeFrameFiltered(FakeFrame):
            def __init__(self, rows):
                self.rows = rows
                self.empty = len(rows) == 0

        class FakeSeries(list):
            def mean(self):
                return sum(self) / len(self) if self else 0.0

            def __eq__(self, other):
                return [value == other for value in self]

        summary = score_ctr_batch.build_score_summary(FakeFrame())

        self.assertEqual(summary["row_count"], 4)
        self.assertEqual(summary["top_decile_row_count"], 2)
        self.assertAlmostEqual(summary["actual_ctr"], 0.5)
        self.assertAlmostEqual(summary["top_decile_actual_ctr"], 1.0)


if __name__ == "__main__":
    unittest.main()
