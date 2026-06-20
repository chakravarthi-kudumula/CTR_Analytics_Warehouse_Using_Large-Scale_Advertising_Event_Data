from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import incoming_batch


class IncomingBatchTests(unittest.TestCase):
    def test_infer_sample_scale(self) -> None:
        self.assertEqual(incoming_batch.infer_sample_scale(Path("criteo_100k.csv")), "100k")
        self.assertEqual(incoming_batch.infer_sample_scale(Path("criteo_1m.csv")), "1m")
        self.assertEqual(incoming_batch.infer_sample_scale(Path("criteo_5m.csv")), "5m")
        self.assertEqual(incoming_batch.infer_sample_scale(Path("live_batch.csv")), "incoming")

    def test_build_batch_name_normalizes_stem(self) -> None:
        batch_name = incoming_batch.build_batch_name(Path("Live Batch-01.csv"), "abcdef1234567890")
        self.assertEqual(batch_name, "live_batch_01_abcdef123456")

    def test_count_csv_rows_excludes_header(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "sample.csv"
            csv_path.write_text("label\n1\n0\n1\n")
            self.assertEqual(incoming_batch.count_csv_rows(csv_path), 3)

    def test_locate_candidate_file_returns_oldest_supported_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            older = temp_path / "older.csv"
            newer = temp_path / "newer.csv"
            ignored = temp_path / "ignored.txt"
            older.write_text("label\n1\n")
            newer.write_text("label\n0\n")
            ignored.write_text("skip")
            os.utime(older, (1_700_000_000, 1_700_000_000))
            os.utime(newer, (1_700_000_100, 1_700_000_100))

            with mock.patch.object(incoming_batch, "INCOMING_DIR", temp_path):
                candidate = incoming_batch.locate_candidate_file(PROJECT_ROOT, None)

            self.assertEqual(candidate, older)


if __name__ == "__main__":
    unittest.main()
