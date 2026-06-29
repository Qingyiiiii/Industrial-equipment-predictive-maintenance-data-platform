# -*- coding: utf-8 -*-
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import metropt_utils  # noqa: E402


class ProjectConfigContractTest(unittest.TestCase):
    def test_local_config_has_required_sections(self):
        config = metropt_utils.load_metropt_config(str(ROOT / "config" / "metropt_quality.local.yaml"))
        for section in ["project", "metropt", "paths", "spark", "hive", "realtime"]:
            self.assertIn(section, config)
        self.assertEqual(config["project"]["domain"], "metropt_quality")
        self.assertEqual(config["metropt"]["dataset_name"], "MetroPT-3 Dataset")

    def test_local_config_failure_windows_parse(self):
        config = metropt_utils.load_metropt_config(str(ROOT / "config" / "metropt_quality.local.yaml"))
        windows = metropt_utils.parse_failure_windows(config)
        self.assertGreaterEqual(len(windows), 4)
        self.assertTrue(all(len(item) == 3 for item in windows))

    def test_local_config_realtime_contract_keys(self):
        config = metropt_utils.load_metropt_config(str(ROOT / "config" / "metropt_quality.local.yaml"))
        realtime = config["realtime"]
        for key in ["kafka_topic", "kafka_dlq_topic", "redis_url", "redis_risk_key_prefix"]:
            self.assertTrue(str(realtime.get(key, "")).strip(), key)
        self.assertIn("metropt", realtime["kafka_topic"])
        self.assertIn("dlq", realtime["kafka_dlq_topic"])


if __name__ == "__main__":
    unittest.main()

