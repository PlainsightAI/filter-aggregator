# Aggregator

[![PyPI version](https://img.shields.io/pypi/v/filter-vizcal.svg?style=flat-square)](https://pypi.org/project/filter-vizcal/)
[![Docker Version](https://img.shields.io/docker/v/plainsightai/openfilter-vizcal?sort=semver)](https://hub.docker.com/r/plainsightai/openfilter-vizcal)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/PlainsightAI/filter-vizcal/blob/main/LICENSE)

The **Aggregator** is a powerful filter that aggregates numeric and categorical data from upstream filters. It supports various aggregation operations and can work with multiple upstream producers and downstream consumers.

## Features

- **Multiple Aggregation Operations**: Sum, Average, Min, Max, Count, Count Distinct, Median, Standard Deviation, Any, All, Mode operations, and Distinct value collection
- **Flexible Configuration**: Support for nested fields using dot notation, optional forwarding of extra fields, image forwarding capability, upstream data forwarding option, and customizable output key naming
- **Use Cases**: Aggregating metrics from multiple sources, computing statistics across multiple frames, collecting unique values from different sources, combining data from parallel processing pipelines, data normalization and consolidation

## How It Works

The Aggregator processes multiple input frames and performs statistical aggregations on specified fields. Here's how it works:

1. **Input Processing**: Receives frames from multiple upstream sources (e.g., different cameras, sensors)
2. **Data Extraction**: Extracts values from specified fields using dot notation (e.g., `meta.temperature`)
3. **Aggregation**: Applies statistical operations (sum, avg, min, max, etc.) across all input frames
4. **Output Generation**: Creates a main aggregated frame plus optionally forwards original source frames

### Example Pipeline

```mermaid
graph TD
    A[Video Input] --> B[Echo Filter 1]
    A --> C[Echo Filter 2]
    B --> D[FilterAggregator]
    C --> D
    D --> E[Web Visualization]
    
    B -.->|"cam_1 data"| D
    C -.->|"cam_2 data"| D
    D -.->|"aggregated + original frames"| E
```

The aggregator receives data from both echo filters and combines them into statistical summaries.

## Usage

### Using the Demo Script

The `scripts/filter_usage.py` script demonstrates the Aggregator in action with a complete pipeline:

```bash
# Run with default configuration
python scripts/filter_usage.py

# Run with custom output path
python scripts/filter_usage.py --output_path output/my_results.json

# Run with custom video input
VIDEO_INPUT=path/to/your/video.mp4 python scripts/filter_usage.py
```

The script creates a complete pipeline with:
- **Video Input**: Reads video frames from a file
- **Echo Filters**: Two filters that cycle through JSON event data and attach it to video frames
- **FilterAggregator**: Aggregates data from both echo filters
- **Web Visualization**: Serves results at `http://localhost:8002`

### Configuration

The Aggregator can be configured using environment variables:

```bash
FILTER_AGGREGATIONS='{"meta.sheeps":"sum", "meta.door_time":"avg", "meta.states":"distinct"}'
FILTER_FORWARD_EXTRA_FIELDS=true
FILTER_FORWARD_IMAGE=false
FILTER_APPEND_OP_TO_KEY=true
FILTER_FORWARD_UPSTREAM_DATA=true
FILTER_DEBUG=false
```

### Sample Data

The demo script automatically creates sample JSON event files (`input/events_1.json` and `input/events_2.json`) with test data including:
- `sheeps`: Numeric values for counting
- `states`: Categorical values (open/closed)
- `temperature`: Numeric values for averaging
- `pressure`, `humidity`: Additional metrics
- `valid`: Boolean values for logical operations

### Expected Output

When running the demo, you'll see output like this in the terminal:

```
{'cam_1': (Frame(960x540xBGR-jpg), {'meta': {'id': '1', 'sheeps': 4, 'states': 'open', 'temperature': 25.5}}),
 'cam_2': (Frame(960x540xBGR-jpg), {'meta': {'id': '1', 'sheeps': 4, 'states': 'open', 'temperature': 25.5}}),
 'main': (Frame(None), {'_meta': {'frame_count': 1, 'sources': 2},
         'meta': {'sheeps_sum': 8, 'states_distinct': ['open'], 'temperature_avg': 25.5}})}
```

This shows:
- `cam_1`, `cam_2`: Original input frames (when `forward_upstream_data=True`)
- `main`: Aggregated frame with statistical summaries
- `_meta`: Metadata about frame count and source count

## Install

In order to run the filter locally or build/publish the Python wheel we need to install properly:

    virtualenv venv
    source venv/bin/activate
    make install

## Run locally

Now to run the filter locally do:

    make run

Then navigate to `http://localhost:8000` and you should see the video looping.

## Unit tests

It is assumed you have installed the packages necessary to run locally (not in docker). Run:

    make test

## Documentation

For more detailed information, configuration examples, and advanced usage scenarios, see the [comprehensive documentation](https://github.com/PlainsightAI/filter-aggregator/blob/main/docs/overview.md).

## License

See LICENSE file for details.