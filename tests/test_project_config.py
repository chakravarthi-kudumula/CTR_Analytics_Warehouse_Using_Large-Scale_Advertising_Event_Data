from __future__ import annotations

import argparse
import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import project_config


class ProjectConfigTests(unittest.TestCase):
    def test_add_db_connection_args_includes_maintenance_when_requested(self) -> None:
        parser = argparse.ArgumentParser()
        project_config.add_db_connection_args(parser, include_maintenance_database=True)
        args = parser.parse_args([])

        self.assertTrue(hasattr(args, "host"))
        self.assertTrue(hasattr(args, "port"))
        self.assertTrue(hasattr(args, "database"))
        self.assertTrue(hasattr(args, "maintenance_database"))
        self.assertTrue(hasattr(args, "user"))
        self.assertTrue(hasattr(args, "password"))

    def test_ensure_batch_directories_creates_expected_paths(self) -> None:
        incoming_dir, archive_dir, failed_dir = project_config.ensure_batch_directories()

        self.assertEqual(incoming_dir, project_config.INCOMING_DIR)
        self.assertEqual(archive_dir, project_config.ARCHIVE_DIR)
        self.assertEqual(failed_dir, project_config.FAILED_DIR)
        self.assertTrue(incoming_dir.exists())
        self.assertTrue(archive_dir.exists())
        self.assertTrue(failed_dir.exists())

    def test_sample_files_cover_expected_scales(self) -> None:
        self.assertEqual(project_config.SAMPLE_FILES["100k"], "criteo_100k.csv")
        self.assertEqual(project_config.SAMPLE_FILES["1m"], "criteo_1m.csv")
        self.assertEqual(project_config.SAMPLE_FILES["5m"], "criteo_5m.csv")


if __name__ == "__main__":
    unittest.main()
