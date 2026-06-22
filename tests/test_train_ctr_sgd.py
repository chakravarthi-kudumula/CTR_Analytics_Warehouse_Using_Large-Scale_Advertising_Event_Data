from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import train_ctr_sgd


class TrainCtrSgdTests(unittest.TestCase):
    def test_module_constants(self) -> None:
        self.assertEqual(train_ctr_sgd.TARGET_COLUMN, "label")


if __name__ == "__main__":
    unittest.main()
