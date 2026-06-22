from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import ml_training_dataset


class MlTrainingDatasetTests(unittest.TestCase):
    def test_parse_day_set(self) -> None:
        self.assertEqual(ml_training_dataset.parse_day_set("1,2,5"), {1, 2, 5})

    def test_parse_day_set_rejects_out_of_range_values(self) -> None:
        with self.assertRaises(ValueError):
            ml_training_dataset.parse_day_set("0,8")

    def test_validate_splits_rejects_overlap(self) -> None:
        with self.assertRaises(ValueError):
            ml_training_dataset.validate_splits({1, 2}, {2, 6}, {7})

    def test_build_split_lookup(self) -> None:
        lookup = ml_training_dataset.build_split_lookup({1, 2, 3}, {6}, {7})
        self.assertEqual(lookup[1], "train")
        self.assertEqual(lookup[6], "validation")
        self.assertEqual(lookup[7], "test")


if __name__ == "__main__":
    unittest.main()
