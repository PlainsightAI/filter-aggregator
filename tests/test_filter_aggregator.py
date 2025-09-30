#!/usr/bin/env python

import logging
import multiprocessing
import os
import sys
import unittest
from unittest.mock import MagicMock
import numpy as np

from filter_aggregator.filter import FilterAggregator, FilterAggregatorConfig
from openfilter.filter_runtime.frame import Frame

logger = logging.getLogger(__name__)

logger.setLevel(int(getattr(logging, (os.getenv('LOG_LEVEL') or 'INFO').upper())))

VERBOSE   = '-v' in sys.argv or '--verbose' in sys.argv
LOG_LEVEL = logger.getEffectiveLevel()


class TestFilterAggregator(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Clean up any existing filter
        if hasattr(self, 'filter') and self.filter:
            try:
                self.filter.shutdown()
            except:
                pass
        
        self.config = FilterAggregatorConfig(
            aggregations={
                "meta.sheeps": "sum",
                "meta.door_time": "avg",
                "meta.states": "distinct",
                "meta.temperature": "min",
                "meta.pressure": "max",
                "meta.valid": "all"
            },
            forward_extra_fields=True,
            forward_image=False,
            append_op_to_key=True,
            forward_upstream_data=True,
            debug=False
        )
        self.filter = FilterAggregator(self.config)
        self.filter.setup(self.config)
    
    def tearDown(self):
        """Clean up after each test."""
        if hasattr(self, 'filter') and self.filter:
            self.filter.shutdown()

    def test_basic_aggregation(self):
        """Test basic aggregation operations with multiple frames."""
        frames = {
            "source1": Frame(
                image=None,
                data={
                    "meta": {
                        "id": 1,
                        "sheeps": 4,
                        "door_time": 10,
                        "states": "open",
                        "temperature": 25,
                        "pressure": 1000,
                        "valid": True
                    }
                },
                format=None
            ),
            "source2": Frame(
                image=None,
                data={
                    "meta": {
                        "id": 2,
                        "sheeps": 5,
                        "door_time": 20,
                        "states": "closed",
                        "temperature": 20,
                        "pressure": 1100,
                        "valid": True
                    }
                },
                format=None
            )
        }

        result = self.filter.process(frames)
        agg_frame = result["main"]

        self.assertEqual(agg_frame.data["meta"]["sheeps_sum"], 9)
        self.assertEqual(agg_frame.data["meta"]["door_time_avg"], 15)
        self.assertEqual(set(agg_frame.data["meta"]["states_distinct"]), {"open", "closed"})
        self.assertEqual(agg_frame.data["meta"]["temperature_min"], 20)
        self.assertEqual(agg_frame.data["meta"]["pressure_max"], 1100)
        self.assertTrue(agg_frame.data["meta"]["valid_all"])

    def test_numeric_aggregation_with_mixed_data(self):
        """Test that numeric aggregations properly filter out non-numeric values."""
        config = FilterAggregatorConfig(
            aggregations={
                "meta.temperature": "avg",
                "meta.pressure": "min",
                "meta.humidity": "max",
                "meta.count": "sum"
            }
        )
        self.filter = FilterAggregator(config)
        self.filter.setup(config)

        frames = {
            "source1": Frame(
                image=None,
                data={
                    "meta": {
                        "temperature": 25.5,
                        "pressure": 1000,
                        "humidity": 60,
                        "count": 10,
                        "status": "active"  # Non-numeric field
                    }
                },
                format=None
            ),
            "source2": Frame(
                image=None,
                data={
                    "meta": {
                        "temperature": None,  # None value should be filtered out
                        "pressure": 1100,
                        "humidity": 65,
                        "count": 20,
                        "status": "inactive"
                    }
                },
                format=None
            ),
            "source3": Frame(
                image=None,
                data={
                    "meta": {
                        "temperature": 24.8,
                        "pressure": 950,
                        "humidity": 55,
                        "count": 15,
                        "status": "pending"
                    }
                },
                format=None
            )
        }

        result = self.filter.process(frames)
        agg_frame = result["main"]

        # Temperature avg should only consider 25.5 and 24.8 (filter out None)
        expected_temp_avg = (25.5 + 24.8) / 2
        self.assertAlmostEqual(agg_frame.data["meta"]["temperature_avg"], expected_temp_avg, places=2)
        
        # Pressure min should be 950
        self.assertEqual(agg_frame.data["meta"]["pressure_min"], 950)
        
        # Humidity max should be 65
        self.assertEqual(agg_frame.data["meta"]["humidity_max"], 65)
        
        # Count sum should be 45
        self.assertEqual(agg_frame.data["meta"]["count_sum"], 45)

    def test_nested_field_handling(self):
        """Test handling of deeply nested fields."""
        config = FilterAggregatorConfig(
            aggregations={"deeply.nested.field": "sum"},
            forward_extra_fields=True
        )
        self.filter = FilterAggregator(config)
        self.filter.setup(config)

        frames = {
            "source1": Frame(
                image=None,
                data={
                    "meta": {"id": 1},
                    "deeply": {"nested": {"field": 10}}
                },
                format=None
            ),
            "source2": Frame(
                image=None,
                data={
                    "meta": {"id": 2},
                    "deeply": {"nested": {"field": 20}}
                },
                format=None
            )
        }

        result = self.filter.process(frames)
        self.assertEqual(result["main"].data["deeply"]["nested"]["field_sum"], 30)

    def test_forward_extra_fields(self):
        """Test forwarding of non-aggregated fields."""
        config = FilterAggregatorConfig(
            aggregations={"meta.value": "sum"},
            forward_extra_fields=True
        )
        self.filter = FilterAggregator(config)
        self.filter.setup(config)

        frames = {
            "source1": Frame(
                image=None,
                data={
                    "meta": {"id": 1, "value": 10},
                    "extra": "data"
                },
                format=None
            )
        }

        result = self.filter.process(frames)
        self.assertEqual(result["main"].data["extra"], "data")

    def test_forward_upstream_data(self):
        """Test forwarding of upstream data when enabled."""
        config = FilterAggregatorConfig(
            aggregations={"meta.value": "sum"},
            forward_upstream_data=True
        )
        self.filter = FilterAggregator(config)
        self.filter.setup(config)

        frames = {
            "source1": Frame(
                image=None,
                data={"meta": {"id": 1, "value": 10}},
                format=None
            ),
            "source2": Frame(
                image=None,
                data={"meta": {"id": 2, "value": 20}},
                format=None
            )
        }

        result = self.filter.process(frames)
        self.assertIn("main", result)
        self.assertEqual(result["main"].data["meta"]["value_sum"], 30)
        # Source frames should be forwarded when forward_upstream_data=True
        self.assertIn("source1", result)
        self.assertIn("source2", result)

    def test_forward_upstream_data_disabled(self):
        """Test that upstream data is not forwarded when disabled."""
        config = FilterAggregatorConfig(
            aggregations={"meta.value": "sum"},
            forward_upstream_data=False
        )
        self.filter = FilterAggregator(config)
        self.filter.setup(config)

        frames = {
            "source1": Frame(
                image=None,
                data={"meta": {"id": 1, "value": 10}},
                format=None
            ),
            "source2": Frame(
                image=None,
                data={"meta": {"id": 2, "value": 20}},
                format=None
            )
        }

        result = self.filter.process(frames)
        self.assertIn("main", result)
        self.assertEqual(result["main"].data["meta"]["value_sum"], 30)
        # Source frames should NOT be forwarded when forward_upstream_data=False
        self.assertNotIn("source1", result)
        self.assertNotIn("source2", result)

    def test_invalid_operation(self):
        """Test handling of invalid aggregation operations."""
        config = FilterAggregatorConfig(
            aggregations={"meta.value": "invalid_op"}
        )
        with self.assertRaises(ValueError):
            self.filter = FilterAggregator(config)
            self.filter.setup(config)

    def test_missing_fields(self):
        """Test handling of missing fields in frames."""
        frames = {
            "source1": Frame(
                image=None,
                data={"meta": {"id": 1, "other": "value"}},
                format=None
            )
        }

        result = self.filter.process(frames)
        agg_frame = result["main"]
        # Should handle missing fields gracefully
        self.assertEqual(agg_frame.data["meta"]["sheeps_sum"], 0)
        self.assertIsNone(agg_frame.data["meta"]["door_time_avg"])
        self.assertEqual(agg_frame.data["meta"]["states_distinct"], [])
        self.assertIsNone(agg_frame.data["meta"]["temperature_min"])
        self.assertIsNone(agg_frame.data["meta"]["pressure_max"])
        self.assertTrue(agg_frame.data["meta"]["valid_all"])

    def test_forward_image(self):
        """Test image forwarding functionality."""
        config = FilterAggregatorConfig(
            aggregations={"meta.value": "sum"},
            forward_image=True
        )
        self.filter = FilterAggregator(config)
        self.filter.setup(config)

        test_image = np.zeros((10, 10, 3), dtype=np.uint8)
        frames = {
            "source1": Frame(
                image=test_image,
                data={"meta": {"id": 1, "value": 10}},
                format="RGB"
            )
        }

        result = self.filter.process(frames)
        self.assertEqual(result["main"].image.tolist(), test_image.tolist())
        self.assertEqual(result["main"].format, "RGB")

    def test_append_op_to_key_disabled(self):
        """Test that operation names are not appended to keys when disabled."""
        config = FilterAggregatorConfig(
            aggregations={"meta.value": "sum"},
            append_op_to_key=False
        )
        self.filter = FilterAggregator(config)
        self.filter.setup(config)

        frames = {
            "source1": Frame(
                image=None,
                data={"meta": {"id": 1, "value": 10}},
                format=None
            )
        }

        result = self.filter.process(frames)
        # Key should be "meta.value" not "meta.value_sum"
        self.assertIn("main", result)
        self.assertIn("meta", result["main"].data)
        self.assertIn("value", result["main"].data["meta"])
        self.assertNotIn("value_sum", result["main"].data["meta"])

    def test_empty_frames(self):
        """Test handling of empty or None frames."""
        frames = {
            "source1": None,
            "source2": Frame(image=None, data={}, format=None)
        }

        result = self.filter.process(frames)
        agg_frame = result["main"]
        
        # Should handle empty frames gracefully
        self.assertEqual(agg_frame.data["meta"]["sheeps_sum"], 0)
        self.assertIsNone(agg_frame.data["meta"]["door_time_avg"])
        self.assertEqual(agg_frame.data["meta"]["states_distinct"], [])
        self.assertIsNone(agg_frame.data["meta"]["temperature_min"])
        self.assertIsNone(agg_frame.data["meta"]["pressure_max"])
        self.assertTrue(agg_frame.data["meta"]["valid_all"])

    def test_main_topic_ordering(self):
        """Test that main topic always comes first in output dictionary."""
        frames = {
            "source2": Frame(image=None, data={"meta": {"value": 20}}, format=None),
            "source1": Frame(image=None, data={"meta": {"value": 10}}, format=None),
            "other": Frame(image=None, data={"meta": {"value": 30}}, format=None)
        }

        result = self.filter.process(frames)
        output_keys = list(result.keys())
        self.assertEqual(output_keys[0], "main")

    def test_statistical_operations(self):
        """Test statistical operations with realistic data."""
        # Test each operation separately to avoid duplicate key issues
        temperatures = [25.5, 26.0, 24.8, 25.2, 23.0, 27.5, 22.0, 28.0]
        frames = {}
        for i, temp in enumerate(temperatures):
            frames[f"source{i+1}"] = Frame(
                image=None,
                data={"meta": {"temperature": temp}},
                format=None
            )

        # Test average
        config_avg = FilterAggregatorConfig(aggregations={"meta.temperature": "avg"})
        self.filter = FilterAggregator(config_avg)
        self.filter.setup(config_avg)
        result = self.filter.process(frames)
        expected_avg = sum(temperatures) / len(temperatures)
        self.assertAlmostEqual(result["main"].data["meta"]["temperature_avg"], expected_avg, places=2)
        
        # Test min
        config_min = FilterAggregatorConfig(aggregations={"meta.temperature": "min"})
        self.filter = FilterAggregator(config_min)
        self.filter.setup(config_min)
        result = self.filter.process(frames)
        expected_min = min(temperatures)
        self.assertEqual(result["main"].data["meta"]["temperature_min"], expected_min)
        
        # Test max
        config_max = FilterAggregatorConfig(aggregations={"meta.temperature": "max"})
        self.filter = FilterAggregator(config_max)
        self.filter.setup(config_max)
        result = self.filter.process(frames)
        expected_max = max(temperatures)
        self.assertEqual(result["main"].data["meta"]["temperature_max"], expected_max)

    def test_boolean_aggregations(self):
        """Test boolean aggregation operations."""
        config = FilterAggregatorConfig(
            aggregations={
                "meta.valid": "any"
            }
        )
        self.filter = FilterAggregator(config)
        self.filter.setup(config)

        frames = {
            "source1": Frame(image=None, data={"meta": {"valid": True}}, format=None),
            "source2": Frame(image=None, data={"meta": {"valid": False}}, format=None),
            "source3": Frame(image=None, data={"meta": {"valid": True}}, format=None)
        }

        result = self.filter.process(frames)
        agg_frame = result["main"]

        # Any should be True (at least one True)
        self.assertTrue(agg_frame.data["meta"]["valid_any"])
        
        # Test 'all' operation separately
        config_all = FilterAggregatorConfig(
            aggregations={
                "meta.valid": "all"
            }
        )
        self.filter = FilterAggregator(config_all)
        self.filter.setup(config_all)
        
        result_all = self.filter.process(frames)
        agg_frame_all = result_all["main"]
        
        # All should be False (not all are True)
        self.assertFalse(agg_frame_all.data["meta"]["valid_all"])


try:
    multiprocessing.set_start_method('spawn')  # CUDA doesn't like fork()
except Exception:
    pass

if __name__ == '__main__':
    unittest.main()