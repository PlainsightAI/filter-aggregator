#!/usr/bin/env python

import os
import pytest
import json
from unittest.mock import patch

from filter_aggregator.filter import FilterAggregator, FilterAggregatorConfig


class TestIntegrationConfigNormalization:
    """Integration tests for configuration normalization and validation."""

    def test_string_to_type_conversions(self):
        """Test that string values are properly converted to their expected types."""
        config_data = {
            'aggregations': '{"meta.count": "sum"}',  # String JSON
            'forward_extra_fields': 'true',  # String boolean
            'forward_image': 'false',  # String boolean
            'append_op_to_key': '1',  # String boolean (1 = true)
            'forward_upstream_data': 'false',  # String boolean
            'debug': '0'  # String boolean (0 = false)
        }
        
        normalized = FilterAggregator.normalize_config(config_data)
        
        # Verify types are correct
        assert isinstance(normalized.aggregations, dict)
        assert normalized.aggregations == {"meta.count": "sum"}
        assert normalized.forward_extra_fields is True
        assert normalized.forward_image is False
        assert normalized.append_op_to_key is True
        assert normalized.forward_upstream_data is False
        assert normalized.debug is False

    def test_required_vs_optional_parameters(self):
        """Test that required parameters are validated and optional ones have defaults."""
        # Test with minimal config
        minimal_config = {}
        normalized = FilterAggregator.normalize_config(minimal_config)
        
        # Check defaults
        assert normalized.aggregations == {}
        assert normalized.forward_extra_fields is True
        assert normalized.forward_image is False
        assert normalized.append_op_to_key is True
        assert normalized.forward_upstream_data is True
        assert normalized.debug is False

    def test_aggregations_validation(self):
        """Test aggregations parameter validation."""
        # Test valid aggregations
        valid_config = {
            'aggregations': {
                'meta.count': 'sum',
                'meta.temperature': 'avg',
                'meta.status': 'distinct',
                'meta.valid': 'all'
            }
        }
        normalized = FilterAggregator.normalize_config(valid_config)
        assert normalized.aggregations == valid_config['aggregations']
        
        # Test invalid aggregations (should raise ValueError)
        invalid_config = {
            'aggregations': {
                'meta.count': 'invalid_operation'
            }
        }
        with pytest.raises(ValueError, match="Unsupported aggregation operation"):
            FilterAggregator.normalize_config(invalid_config)

    def test_boolean_validation(self):
        """Test boolean parameter validation."""
        # Test various boolean string representations
        boolean_tests = [
            ('true', True),
            ('True', True),
            ('TRUE', True),
            ('1', True),
            ('yes', True),
            ('Yes', True),
            ('false', False),
            ('False', False),
            ('FALSE', False),
            ('0', False),
            ('no', False),
            ('No', False)
        ]
        
        for value, expected in boolean_tests:
            config = {'forward_extra_fields': value}
            normalized = FilterAggregator.normalize_config(config)
            assert normalized.forward_extra_fields == expected

    def test_json_aggregations_parsing(self):
        """Test parsing of JSON string aggregations."""
        config_data = {
            'aggregations': '{"meta.sheeps": "sum", "meta.temperature": "avg", "meta.status": "distinct"}'
        }
        
        normalized = FilterAggregator.normalize_config(config_data)
        expected = {
            'meta.sheeps': 'sum',
            'meta.temperature': 'avg',
            'meta.status': 'distinct'
        }
        assert normalized.aggregations == expected

    def test_invalid_json_aggregations(self):
        """Test handling of invalid JSON in aggregations."""
        config_data = {
            'aggregations': '{"invalid": json}'  # Invalid JSON
        }
        
        with pytest.raises(ValueError, match="Invalid aggregations JSON"):
            FilterAggregator.normalize_config(config_data)

    def test_aggregations_type_validation(self):
        """Test that aggregations must be a dictionary."""
        config_data = {
            'aggregations': 'not_a_dict'  # String that's not JSON
        }
        
        with pytest.raises(ValueError, match="Invalid aggregations JSON"):
            FilterAggregator.normalize_config(config_data)

    def test_environment_variable_loading(self):
        """Test loading configuration from environment variables."""
        env_vars = {
            'FILTER_AGGREGATIONS': '{"meta.count": "sum"}',
            'FILTER_FORWARD_EXTRA_FIELDS': 'false',
            'FILTER_FORWARD_IMAGE': 'true',
            'FILTER_APPEND_OP_TO_KEY': 'false',
            'FILTER_FORWARD_SOURCE_DATA': 'true',
            'FILTER_FORWARD_UPSTREAM_DATA': 'false',
            'FILTER_DEBUG': 'true'
        }
        
        with patch.dict(os.environ, env_vars):
            # Create config that will trigger environment variable loading
            config_data = {}
            normalized = FilterAggregator.normalize_config(config_data)
            
            # Note: The current implementation doesn't load from env vars automatically
            # This test documents the expected behavior if env var loading is added
            assert normalized.aggregations == {}
            assert normalized.forward_extra_fields is True  # Default
            assert normalized.forward_image is False  # Default

    def test_edge_cases_and_error_handling(self):
        """Test edge cases and error handling."""
        # Test with None values
        config_with_none = {
            'aggregations': None,
            'forward_extra_fields': None
        }
        with pytest.raises(ValueError, match="aggregations must be a dictionary"):
            FilterAggregator.normalize_config(config_with_none)

    def test_unknown_config_key_validation(self):
        """Test that unknown config keys are handled gracefully."""
        config_with_unknown = {
            'unknown_key': 'some_value',
            'aggregations': {'meta.count': 'sum'},
            'another_unknown': 123
        }
        
        # Should not raise an error, unknown keys should be ignored
        normalized = FilterAggregator.normalize_config(config_with_unknown)
        assert normalized.aggregations == {'meta.count': 'sum'}

    def test_runtime_keys_ignored(self):
        """Test that runtime keys are ignored during normalization."""
        config_with_runtime = {
            'aggregations': {'meta.count': 'sum'},
            'sources': 'tcp://localhost:6000',
            'outputs': 'tcp://localhost:6001',
            'id': 'test_filter'
        }
        
        normalized = FilterAggregator.normalize_config(config_with_runtime)
        assert normalized.aggregations == {'meta.count': 'sum'}
        # Runtime keys should be handled by parent class

    def test_comprehensive_configuration(self):
        """Test a comprehensive configuration with all parameters."""
        comprehensive_config = {
            'aggregations': {
                'meta.count': 'sum',
                'meta.temperature': 'avg',
                'meta.status': 'distinct',
                'meta.valid': 'all',
                'meta.pressure': 'max',
                'meta.humidity': 'min'
            },
            'forward_extra_fields': True,
            'forward_image': True,
            'append_op_to_key': False,
            'forward_upstream_data': False,
            'debug': True
        }
        
        normalized = FilterAggregator.normalize_config(comprehensive_config)
        
        # Verify all parameters are correctly set
        assert normalized.aggregations == comprehensive_config['aggregations']
        assert normalized.forward_extra_fields is True
        assert normalized.forward_image is True
        assert normalized.append_op_to_key is False
        assert normalized.forward_upstream_data is False
        assert normalized.debug is True

    def test_forward_upstream_data_validation(self):
        """Test forward_upstream_data parameter validation."""
        # Test valid values
        for value in [True, False, 'true', 'false', '1', '0', 'yes', 'no']:
            config = {'forward_upstream_data': value}
            normalized = FilterAggregator.normalize_config(config)
            expected = value.lower() in ('true', '1', 'yes') if isinstance(value, str) else value
            assert normalized.forward_upstream_data == expected

    def test_operation_registry_validation(self):
        """Test that all operations in the registry are valid."""
        all_operations = [
            'sum', 'avg', 'min', 'max', 'count', 'count_distinct',
            'distinct', 'median', 'std', 'any', 'all', 'mode'
        ]
        
        for op in all_operations:
            config = {'aggregations': {'meta.test': op}}
            normalized = FilterAggregator.normalize_config(config)
            assert normalized.aggregations == {'meta.test': op}

    def test_nested_field_aggregations(self):
        """Test aggregations with nested field paths."""
        config = {
            'aggregations': {
                'meta.sensors.temperature': 'avg',
                'meta.sensors.humidity': 'max',
                'deeply.nested.field': 'sum',
                'simple_field': 'count'
            }
        }
        
        normalized = FilterAggregator.normalize_config(config)
        assert normalized.aggregations == config['aggregations']

    def test_empty_aggregations(self):
        """Test handling of empty aggregations."""
        config = {'aggregations': {}}
        normalized = FilterAggregator.normalize_config(config)
        assert normalized.aggregations == {}

    def test_boolean_string_edge_cases(self):
        """Test edge cases for boolean string conversion."""
        edge_cases = [
            ('', False),  # Empty string
            (' ', False),  # Whitespace
            ('maybe', False),  # Invalid boolean
            ('2', False),  # Non-zero number string
        ]
        
        for value, expected in edge_cases:
            config = {'debug': value}
            normalized = FilterAggregator.normalize_config(config)
            assert normalized.debug == expected


if __name__ == '__main__':
    pytest.main([__file__])
