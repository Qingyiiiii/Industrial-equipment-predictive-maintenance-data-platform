# -*- coding: utf-8 -*-
import os
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import metropt_utils  # noqa: E402


class MetroptUtilsLightTest(unittest.TestCase):
    def test_simple_yaml_load_preserves_quoted_hash(self):
        payload = """
project:
  domain: metropt_quality
note:
  text: "keep # inside quotes"
enabled: true
count: 3
"""
        data = metropt_utils._simple_yaml_load(payload)
        self.assertEqual(data["project"]["domain"], "metropt_quality")
        self.assertEqual(data["note"]["text"], "keep # inside quotes")
        self.assertTrue(data["enabled"])
        self.assertEqual(data["count"], 3)

    def test_parse_failure_windows(self):
        cfg = {
            "metropt": {
                "failure_windows": (
                    "2020-04-18 00:00:00|2020-04-18 23:59:59|air_leak;"
                    "2020-07-15 14:30:00|2020-07-15 19:00:00|air_leak"
                )
            }
        }
        windows = metropt_utils.parse_failure_windows(cfg)
        self.assertEqual(len(windows), 2)
        self.assertEqual(windows[0][2], "air_leak")

    def test_sensor_dimension_rows_cover_all_sensors(self):
        sensors = metropt_utils.ANALOG_SENSORS + metropt_utils.DIGITAL_SENSORS
        rows = metropt_utils.sensor_dimension_rows(sensors)
        self.assertEqual(len(rows), 15)
        by_name = {row["sensor_name"]: row for row in rows}
        self.assertEqual(by_name["tp2"]["sensor_type"], "analog")
        self.assertEqual(by_name["dv_electric"]["sensor_type"], "digital")
        self.assertEqual(by_name["motor_current"]["station_id"], "compressor_motor")

    def test_join_path_keeps_uri_prefix(self):
        self.assertEqual(
            metropt_utils.join_path("hdfs:///tmp/metropt", "ods", "readings"),
            "hdfs:///tmp/metropt/ods/readings",
        )
        local = metropt_utils.join_path("D:/path/to/project", "data", "metropt_quality")
        self.assertTrue(local.endswith(os.path.join("data", "metropt_quality")))

    def test_sql_identifier_rejects_unsafe_text(self):
        self.assertEqual(metropt_utils.sql_identifier("metropt_quality"), "metropt_quality")
        with self.assertRaises(ValueError):
            metropt_utils.sql_identifier("metropt_quality; drop table x")


if __name__ == "__main__":
    unittest.main()

