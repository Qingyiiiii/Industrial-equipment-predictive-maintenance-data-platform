# -*- coding: utf-8 -*-
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
STREAMING_DIR = ROOT / "streaming"
if str(STREAMING_DIR) not in sys.path:
    sys.path.insert(0, str(STREAMING_DIR))

import metropt_realtime_risk_score_plan as contract  # noqa: E402


class RealtimeRiskContractTest(unittest.TestCase):
    def test_sample_valid_events_pass_contract(self):
        for event in contract.sample_valid_events():
            self.assertEqual(contract.validate_event(event), [])

    def test_missing_required_field_is_reported(self):
        event = dict(contract.sample_valid_events()[0])
        event.pop("event_time")
        errors = contract.validate_event(event)
        self.assertIn("missing_required_field:event_time", errors)

    def test_non_numeric_sensor_is_reported(self):
        event = dict(contract.sample_valid_events()[0])
        event["motor_current"] = "bad"
        errors = contract.validate_event(event)
        self.assertIn("non_numeric_sensor:motor_current", errors)

    def test_enrich_event_marks_signal_proxy_boundary(self):
        event = contract.sample_valid_events()[-1]
        enriched = contract.enrich_event(event)
        self.assertIn("risk_score", enriched)
        self.assertIn(enriched["risk_level"], {"low", "medium", "high"})
        self.assertEqual(enriched["risk_score_source"], "dry_run_signal_proxy_not_production")
        self.assertEqual(enriched["risk_model_name"], "p9_realtime_contract")

    def test_existing_risk_score_is_clipped_and_passed_through(self):
        event = dict(contract.sample_valid_events()[0])
        event["risk_score"] = 2.0
        score, reasons, source = contract.dry_run_risk_score(event)
        self.assertEqual(score, 1.0)
        self.assertEqual(reasons, ["passed_through_existing_risk_score"])
        self.assertEqual(source, "provided_risk_score")


if __name__ == "__main__":
    unittest.main()

