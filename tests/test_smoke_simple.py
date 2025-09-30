#!/usr/bin/env python

import os
import tempfile
import pytest
import numpy as np
from unittest.mock import patch

from filter_aggregator.filter import FilterAggregator, FilterAggregatorConfig, Frame


class TestSmokeSimple:
    """Simple smoke tests for basic filter functionality."""

    def test_filter_initialization(self):
        """Test that the filter can be initialized with valid config."""
        config_data = {
            'aggregations': {'meta.count': 'sum'},
            'forward_extra_fields': True,
            'forward_image': False,
            'append_op_to_key': True,
            'forward_upstream_data': True,
            'debug': False
        }
        
        config = FilterAggregator.normalize_config(config_data)
        filter_instance = FilterAggregator(config=config)
        filter_instance.setup(config)
        
        assert filter_instance is not None
        assert filter_instance.cfg.aggregations == {'meta.count': 'sum'}

    def test_setup_and_shutdown(self):
        """Test that setup() and shutdown() work correctly."""
        config_data = {
            'aggregations': {'meta.count': 'sum'},
            'debug': True
        }
        
        config = FilterAggregator.normalize_config(config_data)
        filter_instance = FilterAggregator(config=config)
        
        # Test setup
        filter_instance.setup(config)
        assert filter_instance.cfg.aggregations == {'meta.count': 'sum'}
        assert filter_instance._frame_ctr == 0
        
        # Test shutdown
        filter_instance.shutdown()  # Should not raise any exceptions

    def test_config_validation(self):
        """Test that config validation works correctly."""
        config_data = {
            'aggregations': '{"meta.count": "sum"}',  # String that should be converted
            'forward_extra_fields': 'true',  # String that should be converted
            'forward_image': 'false',  # String that should be converted
            'append_op_to_key': '1',  # String that should be converted
            'forward_upstream_data': 'false',  # String that should be converted
            'debug': '0'  # String that should be converted
        }
        
        config = FilterAggregator.normalize_config(config_data)
        assert config.aggregations == {"meta.count": "sum"}
        assert config.forward_extra_fields is True
        assert config.forward_image is False
        assert config.append_op_to_key is True
        assert config.forward_upstream_data is False
        assert config.debug is False

    def test_basic_aggregation_processing(self):
        """Test basic aggregation processing with multiple frames."""
        config_data = {
            'aggregations': {
                'meta.count': 'sum',
                'meta.temperature': 'avg',
                'meta.status': 'distinct'
            },
            'forward_extra_fields': True,
            'forward_image': False,
            'append_op_to_key': True,
            'forward_upstream_data': True,
            'debug': False
        }
        
        config = FilterAggregator.normalize_config(config_data)
        filter_instance = FilterAggregator(config=config)
        filter_instance.setup(config)
        
        # Create test frames
        frames = {
            "source1": Frame(
                image=np.zeros((10, 10, 3), dtype=np.uint8),
                data={
                    "meta": {
                        "id": 1,
                        "count": 5,
                        "temperature": 20.0,
                        "status": "active"
                    }
                },
                format="BGR"
            ),
            "source2": Frame(
                image=np.zeros((10, 10, 3), dtype=np.uint8),
                data={
                    "meta": {
                        "id": 2,
                        "count": 3,
                        "temperature": 30.0,
                        "status": "inactive"
                    }
                },
                format="BGR"
            )
        }
        
        result = filter_instance.process(frames)
        
        # Verify output structure
        assert "main" in result
        assert isinstance(result["main"], Frame)
        
        # Verify aggregation results
        main_data = result["main"].data
        assert main_data["meta"]["count_sum"] == 8
        assert main_data["meta"]["temperature_avg"] == 25.0
        assert set(main_data["meta"]["status_distinct"]) == {"active", "inactive"}

    def test_empty_frame_processing(self):
        """Test processing with empty frame dictionary."""
        config_data = {
            'aggregations': {'meta.count': 'sum'},
            'debug': True
        }
        
        config = FilterAggregator.normalize_config(config_data)
        filter_instance = FilterAggregator(config=config)
        filter_instance.setup(config)
        
        # Process empty frames
        frames = {}
        result = filter_instance.process(frames)
        
        # Should return main frame with aggregated data
        assert 'main' in result
        assert result['main'].data['meta']['count_sum'] == 0

    def test_non_image_frame_forwarding(self):
        """Test forwarding of non-image frames when forward_upstream_data is enabled."""
        config_data = {
            'aggregations': {'meta.count': 'sum'},
            'forward_upstream_data': True,
            'debug': True
        }
        
        config = FilterAggregator.normalize_config(config_data)
        filter_instance = FilterAggregator(config=config)
        filter_instance.setup(config)
        
        # Create frames with and without images
        frames = {
            "main": Frame(
                image=np.zeros((10, 10, 3), dtype=np.uint8),
                data={"meta": {"id": 1, "count": 5}},
                format="BGR"
            ),
            "data_only": Frame(
                image=None,
                data={"some": "data"},
                format=None
            )
        }
        
        result = filter_instance.process(frames)
        
        # Verify both frames are in output
        assert "main" in result
        assert "data_only" in result
        assert result["data_only"].data == {"some": "data"}

    def test_non_image_frame_not_forwarded_when_disabled(self):
        """Test that non-image frames are not forwarded when forward_upstream_data is disabled."""
        config_data = {
            'aggregations': {'meta.count': 'sum'},
            'forward_upstream_data': False,
            'debug': True
        }
        
        config = FilterAggregator.normalize_config(config_data)
        filter_instance = FilterAggregator(config=config)
        filter_instance.setup(config)
        
        # Create frames with and without images
        frames = {
            "main": Frame(
                image=np.zeros((10, 10, 3), dtype=np.uint8),
                data={"meta": {"id": 1, "count": 5}},
                format="BGR"
            ),
            "data_only": Frame(
                image=None,
                data={"some": "data"},
                format=None
            )
        }
        
        result = filter_instance.process(frames)
        
        # Verify only main frame is in output
        assert "main" in result
        assert "data_only" not in result

    def test_main_topic_ordering(self):
        """Test that main topic always comes first in output dictionary."""
        config_data = {
            'aggregations': {'meta.count': 'sum'},
            'forward_upstream_data': True,
            'debug': True
        }
        
        config = FilterAggregator.normalize_config(config_data)
        filter_instance = FilterAggregator(config=config)
        filter_instance.setup(config)
        
        # Process frames with multiple topics in different order
        frames = {
            "source2": Frame(
                image=np.zeros((10, 10, 3), dtype=np.uint8),
                data={"meta": {"id": 2, "count": 3}},
                format="BGR"
            ),
            "main": Frame(
                image=np.zeros((10, 10, 3), dtype=np.uint8),
                data={"meta": {"id": 1, "count": 5}},
                format="BGR"
            ),
            "source3": Frame(
                image=np.zeros((10, 10, 3), dtype=np.uint8),
                data={"meta": {"id": 3, "count": 2}},
                format="BGR"
            )
        }
        
        result = filter_instance.process(frames)
        
        # Verify main topic comes first regardless of input order
        output_keys = list(result.keys())
        assert output_keys[0] == "main"
        assert len(result) >= 1  # At least main should be present


    def test_image_forwarding(self):
        """Test image forwarding when enabled."""
        config_data = {
            'aggregations': {'meta.count': 'sum'},
            'forward_image': True,
            'debug': True
        }
        
        config = FilterAggregator.normalize_config(config_data)
        filter_instance = FilterAggregator(config=config)
        filter_instance.setup(config)
        
        test_image = np.zeros((10, 10, 3), dtype=np.uint8)
        frames = {
            "main": Frame(
                image=test_image,
                data={"meta": {"id": 1, "count": 5}},
                format="BGR"
            )
        }
        
        result = filter_instance.process(frames)
        
        # Verify image is forwarded
        assert "main" in result
        assert result["main"].image is not None
        assert result["main"].format == "BGR"

    def test_extra_fields_forwarding(self):
        """Test forwarding of extra fields when enabled."""
        config_data = {
            'aggregations': {'meta.count': 'sum'},
            'forward_extra_fields': True,
            'debug': True
        }
        
        config = FilterAggregator.normalize_config(config_data)
        filter_instance = FilterAggregator(config=config)
        filter_instance.setup(config)
        
        frames = {
            "main": Frame(
                image=np.zeros((10, 10, 3), dtype=np.uint8),
                data={
                    "meta": {"id": 1, "count": 5},
                    "extra": {"info": "test"}
                },
                format="BGR"
            )
        }
        
        result = filter_instance.process(frames)
        
        # Verify extra fields are forwarded
        assert "main" in result
        assert result["main"].data["extra"] == {"info": "test"}

    def test_append_op_to_key_disabled(self):
        """Test behavior when append_op_to_key is disabled."""
        config_data = {
            'aggregations': {'meta.count': 'sum'},
            'append_op_to_key': False,
            'debug': True
        }
        
        config = FilterAggregator.normalize_config(config_data)
        filter_instance = FilterAggregator(config=config)
        filter_instance.setup(config)
        
        frames = {
            "main": Frame(
                image=np.zeros((10, 10, 3), dtype=np.uint8),
                data={"meta": {"id": 1, "count": 5}},
                format="BGR"
            )
        }
        
        result = filter_instance.process(frames)
        
        # Verify operation suffix is not appended
        assert "main" in result
        assert "count" in result["main"].data["meta"]  # No "_sum" suffix
        assert result["main"].data["meta"]["count"] == 5

    def test_debug_mode_processing(self):
        """Test processing in debug mode."""
        config_data = {
            'aggregations': {'meta.count': 'sum'},
            'debug': True
        }
        
        config = FilterAggregator.normalize_config(config_data)
        filter_instance = FilterAggregator(config=config)
        filter_instance.setup(config)
        
        frames = {
            "main": Frame(
                image=np.zeros((10, 10, 3), dtype=np.uint8),
                data={"meta": {"id": 1, "count": 5}},
                format="BGR"
            )
        }
        
        # Should not raise any exceptions in debug mode
        result = filter_instance.process(frames)
        assert "main" in result

    def test_string_config_conversion(self):
        """Test that string configs are properly converted to types."""
        config_data = {
            'aggregations': '{"meta.count": "sum"}',
            'forward_extra_fields': 'true',
            'forward_image': 'false',
            'append_op_to_key': '1',
            'forward_upstream_data': 'false',
            'debug': '0'
        }
        
        config = FilterAggregator.normalize_config(config_data)
        filter_instance = FilterAggregator(config=config)
        filter_instance.setup(config)
        
        # Should work without errors
        frames = {
            "main": Frame(
                image=np.zeros((10, 10, 3), dtype=np.uint8),
                data={"meta": {"id": 1, "count": 5}},
                format="BGR"
            )
        }
        
        result = filter_instance.process(frames)
        assert "main" in result

    def test_error_handling_invalid_config(self):
        """Test error handling for invalid configuration values."""
        # Test invalid aggregation operation
        config_invalid_op = {
            'aggregations': {'meta.count': 'invalid_operation'}
        }
        with pytest.raises(ValueError, match="Unsupported aggregation operation"):
            FilterAggregator.normalize_config(config_invalid_op)
        
        # Test invalid JSON aggregations
        config_invalid_json = {
            'aggregations': '{"invalid": json}'
        }
        with pytest.raises(ValueError, match="Invalid aggregations JSON"):
            FilterAggregator.normalize_config(config_invalid_json)

    def test_environment_variable_loading(self):
        """Test environment variable configuration loading."""
        # This test documents expected behavior if env var loading is added
        # Currently the filter doesn't automatically load from environment variables
        config_data = {}
        config = FilterAggregator.normalize_config(config_data)
        
        # Should use defaults
        assert config.aggregations == {}
        assert config.forward_extra_fields is True
        assert config.forward_image is False

    def test_comprehensive_aggregation_operations(self):
        """Test various aggregation operations."""
        config_data = {
            'aggregations': {
                'meta.count': 'sum',
                'meta.temperature': 'avg',
                'meta.pressure': 'min',
                'meta.humidity': 'max',
                'meta.status': 'distinct',
                'meta.valid': 'all',
                'meta.active': 'any',
                'meta.values': 'count'
            },
            'debug': True
        }
        
        config = FilterAggregator.normalize_config(config_data)
        filter_instance = FilterAggregator(config=config)
        filter_instance.setup(config)
        
        frames = {
            "source1": Frame(
                image=np.zeros((10, 10, 3), dtype=np.uint8),
                data={
                    "meta": {
                        "id": 1,
                        "count": 5,
                        "temperature": 20.0,
                        "pressure": 1000,
                        "humidity": 60,
                        "status": "active",
                        "valid": True,
                        "active": False,
                        "values": [1, 2, 3]
                    }
                },
                format="BGR"
            ),
            "source2": Frame(
                image=np.zeros((10, 10, 3), dtype=np.uint8),
                data={
                    "meta": {
                        "id": 2,
                        "count": 3,
                        "temperature": 30.0,
                        "pressure": 800,
                        "humidity": 70,
                        "status": "inactive",
                        "valid": True,
                        "active": True,
                        "values": [4, 5]
                    }
                },
                format="BGR"
            )
        }
        
        result = filter_instance.process(frames)
        
        # Verify all aggregation operations work
        main_data = result["main"].data
        assert main_data["meta"]["count_sum"] == 8
        assert main_data["meta"]["temperature_avg"] == 25.0
        assert main_data["meta"]["pressure_min"] == 800
        assert main_data["meta"]["humidity_max"] == 70
        assert set(main_data["meta"]["status_distinct"]) == {"active", "inactive"}
        assert main_data["meta"]["valid_all"] is True
        assert main_data["meta"]["active_any"] is True
        assert main_data["meta"]["values_count"] == 2


if __name__ == '__main__':
    pytest.main([__file__])
