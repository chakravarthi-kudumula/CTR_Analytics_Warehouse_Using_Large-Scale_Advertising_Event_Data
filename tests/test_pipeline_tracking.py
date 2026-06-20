from __future__ import annotations

import hashlib
import sys
import tempfile
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from pipeline_tracking import compute_file_sha256, sql_literal


class PipelineTrackingTests(unittest.TestCase):
    def test_sql_literal_escapes_single_quotes(self) -> None:
        self.assertEqual(sql_literal("O'Brien"), "O''Brien")

    def test_compute_file_sha256_matches_hashlib(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = Path(temp_dir) / "sample.txt"
            payload = b"ctr analytics warehouse test payload"
            file_path.write_bytes(payload)

            self.assertEqual(
                compute_file_sha256(file_path),
                hashlib.sha256(payload).hexdigest(),
            )


if __name__ == "__main__":
    unittest.main()
