import logging
import statistics
from collections import defaultdict
from typing import Any, Callable, Dict, List, Union

from openfilter.filter_runtime.filter import FilterConfig, Filter, Frame

__all__ = ['FilterAggregatorConfig', 'FilterAggregator']

logger = logging.getLogger(__name__)

class FilterAggregatorConfig(FilterConfig):
    """Configuration for the FilterAggregator.

    Attributes:
        aggregations: Map of field names to aggregation operations. Supports dot notation for nested fields.
            e.g. {"meta.sheeps": "sum", "meta.states": "distinct"}
        forward_extra_fields: Whether to forward fields not in aggregations
        forward_image: Whether to forward the image from input frames
        append_op_to_key: Whether to append operation name to output keys
        forward_upstream_data: Whether to forward data from upstream filters
        debug: Enable verbose logging
    """
    aggregations: Dict[str, str] = {}  # e.g. {"meta.sheeps": "sum"}
    forward_extra_fields: bool = True  # echo untouched keys
    forward_image: bool = False  # include Frame.image?
    append_op_to_key: bool = True  # True -> 'sheeps_sum', False -> 'sheeps'
    forward_upstream_data: bool = True  # Forward data from upstream filters
    debug: bool = False  # verbose logging


class FilterAggregator(Filter):
    """Filter that aggregates numeric & categorical data from upstream filters.

    This filter supports various aggregation operations like sum, avg, min, max,
    count, count_distinct, median, std, any, all, and mode. It can work with
    multiple upstream producers and downstream consumers.

    Features:
    - Multiple aggregation operations (sum, avg, min, max, count, etc.)
    - Support for nested fields using dot notation
    - Forward data from upstream filters (configurable)
    - Forward extra fields and images
    - Main topic always comes first in output

    Example:
        Config:
            FILTER_AGGREGATIONS='{"meta.sheeps":"sum", "meta.door_time":"avg", "meta.states":"distinct"}'
            FILTER_FORWARD_EXTRA_FIELDS=false
            FILTER_FORWARD_IMAGE=false
            FILTER_FORWARD_UPSTREAM_DATA=true

        Input frames:
            Frame1: {"meta": {"sheeps": 4, "states": "open"}}
            Frame2: {"meta": {"sheeps": 5, "states": "closed"}}

        Output frame:
            {"main": {"meta.sheeps_sum": 9, "meta.states_distinct": ["open", "closed"]}}
    """

    # Registry maps operation names to their implementation
    _OP_REGISTRY: Dict[str, Callable[[List[Any]], Any]] = {
        "sum": lambda xs: sum(x for x in xs if isinstance(x, (int, float))),
        "avg": lambda xs: (sum(x for x in xs if isinstance(x, (int, float))) / len([x for x in xs if isinstance(x, (int, float))])) if [x for x in xs if isinstance(x, (int, float))] else None,
        "min": lambda xs: min(x for x in xs if isinstance(x, (int, float))) if [x for x in xs if isinstance(x, (int, float))] else None,
        "max": lambda xs: max(x for x in xs if isinstance(x, (int, float))) if [x for x in xs if isinstance(x, (int, float))] else None,
        "count": lambda xs: len(xs),
        "count_distinct": lambda xs: len(set(xs)),
        "distinct": lambda xs: list(dict.fromkeys(xs)),  # order-preserving
        "median": lambda xs: statistics.median([x for x in xs if isinstance(x, (int, float))]) if [x for x in xs if isinstance(x, (int, float))] else None,
        "std": lambda xs: statistics.stdev([x for x in xs if isinstance(x, (int, float))]) if len([x for x in xs if isinstance(x, (int, float))]) > 1 else 0,
        "any": lambda xs: any(xs),
        "all": lambda xs: all(xs),
        "mode": lambda xs: statistics.mode(xs) if xs else None,
    }

    @classmethod
    def normalize_config(cls, config: FilterAggregatorConfig) -> FilterAggregatorConfig:
        """Normalize and validate the configuration.

        Args:
            config: The input configuration to normalize

        Returns:
            Normalized configuration

        Raises:
            ValueError: If configuration is invalid
        """
        config = FilterAggregatorConfig(super().normalize_config(config))

        # Parse aggregations if it's a string (from env var)
        if isinstance(config.aggregations, str):
            try:
                import json
                config.aggregations = json.loads(config.aggregations)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid aggregations JSON: {e}")

        # Validate aggregations
        if not isinstance(config.aggregations, dict):
            raise ValueError("aggregations must be a dictionary")

        # Validate operations
        invalid_ops = [op for op in config.aggregations.values() if op not in cls._OP_REGISTRY]
        if invalid_ops:
            raise ValueError(f"Unsupported aggregation operation(s): {invalid_ops}")

        # Convert string booleans to actual booleans
        for key in ['forward_extra_fields', 'forward_image', 'append_op_to_key', 'forward_upstream_data', 'debug']:
            if isinstance(config.get(key), str):
                config[key] = config[key].lower() in ('true', '1', 'yes')

        return config

    def setup(self, config: FilterAggregatorConfig):
        """Initialize the filter with the given configuration.

        Args:
            config: The normalized configuration
        """
        self.cfg = config
        self._frame_ctr = 0

        if self.cfg.debug:
            logger.info("FilterAggregator initialized with config: %s", {
                'aggregations': self.cfg.aggregations,
                'forward_extra_fields': self.cfg.forward_extra_fields,
                'forward_image': self.cfg.forward_image,
                'append_op_to_key': self.cfg.append_op_to_key
            })

    def _get_nested_value(self, data: dict, path: str) -> Any:
        """Get a value from a nested dictionary using dot notation.
        
        Args:
            data: The dictionary to search in
            path: The dot-notation path to the value
            
        Returns:
            The value at the path, or None if not found
        """
        parts = path.split('.')
        current = data
        for part in parts:
            if not isinstance(current, dict):
                return None
            current = current.get(part)
            if current is None:
                return None
        return current

    def _set_nested_value(self, data: dict, path: str, value: Any) -> None:
        """Set a value in a nested dictionary using dot notation.
        
        Args:
            data: The dictionary to modify
            path: The dot-notation path to set
            value: The value to set
        """
        parts = path.split('.')
        current = data
        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]
        current[parts[-1]] = value

    def process(self, frames: Dict[str, Frame]) -> Dict[str, Frame]:
        """Process incoming frames and return aggregated results.

        Args:
            frames: Dictionary of topic -> Frame mappings

        Returns:
            Dictionary with aggregated Frame and optional source Frames
        """
        logger.info("FilterAggregator processing frames: %s", frames)

        # Initialize output frames dictionary
        output_frames = {}
        
        # Forward frames if upstream forwarding is enabled
        if self.cfg.forward_upstream_data:
            for topic, frame in frames.items():
                if frame is not None:
                    output_frames[topic] = frame

        # Collect values for aggregation from all frames (both image and data-only)
        aggregated: Dict[str, List[Any]] = defaultdict(list)
        
        for topic, frame in frames.items():
            if frame is None:
                continue
                
            for key, op_name in self.cfg.aggregations.items():
                value = self._get_nested_value(frame.data, key)
                if value is not None:
                    aggregated[key].append(value)

        # Initialize output data with extra fields if configured
        out_data = {}
        if self.cfg.forward_extra_fields:
            # Find first frame for exemplar (image or data-only)
            exemplar = None
            for frame in frames.values():
                if frame:
                    exemplar = frame
                    break
            
            if exemplar:
                # Copy only non-aggregated fields
                for key, value in exemplar.data.items():
                    if not any(key.startswith(agg_key.split('.')[0]) for agg_key in self.cfg.aggregations):
                        out_data[key] = value

        # Apply aggregation operations
        errors: List[str] = []

        for key, op_name in self.cfg.aggregations.items():
            op = self._OP_REGISTRY.get(op_name)
            if not op:
                errors.append(f"Unknown operation {op_name} for field {key}")
                continue
            try:
                result = op(aggregated.get(key, []))
                out_key = f"{key}_{op_name}" if self.cfg.append_op_to_key else key
                self._set_nested_value(out_data, out_key, result)
            except Exception as exc:
                errors.append(f"Failed {op_name} on {key}: {exc}")


        if errors:
            logger.warning("Aggregation issues: %s", "; ".join(errors))

        # Get frame count from first available frame
        # Increment frame counter
        self._frame_ctr += 1
        # Add metadata
        out_data['_meta'] = {
            'sources': len([f for f in frames.values() if f and f.has_image]),
            'frame_count': self._frame_ctr
        }

        # Build main output frame - always create one
        exemplar = None
        for frame in frames.values():
            if frame:
                exemplar = frame
                break
        
        # Create main frame with aggregated data
        main_frame = Frame(
            image=exemplar.image if exemplar and exemplar.has_image and self.cfg.forward_image else None,
            data=out_data,
            format=exemplar.format if exemplar and exemplar.has_image and self.cfg.forward_image else None
        )
        output_frames["main"] = main_frame
        

        # Ensure main topic comes first in the output dictionary
        if 'main' in output_frames:
            main_frame = output_frames.pop('main')
            return {'main': main_frame, **output_frames}
        
        return output_frames

    def shutdown(self):
        """Clean up resources and log final statistics."""
        logger.info("FilterAggregator shutting down. Processed %d frames", self._frame_ctr)


if __name__ == '__main__':
    FilterAggregator.run()
