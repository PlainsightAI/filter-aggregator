import argparse
import logging
import os
import json

from openfilter.filter_runtime import Filter
from openfilter.filter_runtime.filters.webvis import Webvis
from openfilter.filter_runtime.filters.video_in import VideoIn
from filter_aggregator import FilterAggregator, FilterAggregatorConfig
from echo_filter import EchoFilter, EchoFilterConfig

def create_test_events():
    """Create test events files with sample data for aggregation."""
    # Ensure input directory exists
    os.makedirs("input", exist_ok=True)
    
    # Create events for first EchoFilter
    events_1 = [
        {"id": "1", "sheeps": 4, "states": "open", "temperature": 25.5, "pressure": 1000, "humidity": 60, "valid": True},
        {"id": "2", "sheeps": 5, "states": "closed", "temperature": 26.0, "pressure": 1100, "humidity": 65, "valid": True},
        {"id": "3", "sheeps": 3, "states": "open", "temperature": 24.8, "pressure": 950, "humidity": 55, "valid": False},
        {"id": "4", "sheeps": 6, "states": "closed", "temperature": 25.2, "pressure": 1050, "humidity": 70, "valid": True}
    ]
    
    # Create events for second EchoFilter
    events_2 = [
        {"id": "5", "sheeps": 2, "states": "open", "temperature": 23.0, "pressure": 900, "humidity": 50, "valid": True},
        {"id": "6", "sheeps": 7, "states": "closed", "temperature": 27.5, "pressure": 1200, "humidity": 75, "valid": True},
        {"id": "7", "sheeps": 1, "states": "open", "temperature": 22.0, "pressure": 850, "humidity": 45, "valid": False},
        {"id": "8", "sheeps": 8, "states": "closed", "temperature": 28.0, "pressure": 1300, "humidity": 80, "valid": True}
    ]
    
    # Write events to files
    with open("input/events_1.json", "w") as f:
        json.dump(events_1, f, indent=2)
    
    with open("input/events_2.json", "w") as f:
        json.dump(events_2, f, indent=2)
    
    print("Created sample data files: input/events_1.json, input/events_2.json")

def main():
    parser = argparse.ArgumentParser(description="Run the FilterAggregator test pipeline.")
    parser.add_argument("--output_path", default="output/aggregated.json", help="Where the aggregated output will be saved.")
    args = parser.parse_args()

    # Create test events if they don't exist
    if not os.path.exists("input/events_1.json") or not os.path.exists("input/events_2.json"):
        create_test_events()

    # Ensure output directory exists
    os.makedirs(os.path.dirname(args.output_path), exist_ok=True)

    # Load aggregations from environment variable or use defaults
    aggregations_str = os.getenv("FILTER_AGGREGATIONS")
    if aggregations_str:
        try:
            aggregations = json.loads(aggregations_str)
        except json.JSONDecodeError as e:
            print(f"Warning: Invalid FILTER_AGGREGATIONS JSON: {e}")
            aggregations = {
                "meta.sheeps": "sum",
                "meta.states": "distinct",
                "meta.temperature": "avg"
            }
    else:
        aggregations = {
            "meta.sheeps": "sum",
            "meta.states": "distinct",
            "meta.temperature": "avg"
        }

    # Load other configuration from environment variables
    forward_extra_fields = os.getenv("FILTER_FORWARD_EXTRA_FIELDS", "True").lower() in ("true", "1", "yes")
    forward_image = os.getenv("FILTER_FORWARD_IMAGE", "False").lower() in ("true", "1", "yes")
    append_op_to_key = os.getenv("FILTER_APPEND_OP_TO_KEY", "True").lower() in ("true", "1", "yes")
    forward_upstream_data = os.getenv("FILTER_FORWARD_UPSTREAM_DATA", "True").lower() in ("true", "1", "yes")
    debug = os.getenv("FILTER_DEBUG", "False").lower() in ("true", "1", "yes")

    # Configure the pipeline
    Filter.run_multi(
        [
            (
                VideoIn,
                dict(
                    id="video_in",
                    sources=f"file://{str(os.getenv('VIDEO_INPUT', '../data/sample-video.mp4'))}!resize=960x540lin!loop",
                    outputs="tcp://*:6000",
                )
            ),
            # Echo filter to cycle through JSON events
            (
                EchoFilter,
                EchoFilterConfig(
                    id="echo",
                    sources="tcp://127.0.0.1:6000",
                    outputs="tcp://*:6002",
                    input_json_path="input/events_1.json"
                )
            ),
            (
                EchoFilter,
                EchoFilterConfig(
                    id="echo",
                    sources="tcp://127.0.0.1:6000",
                    outputs="tcp://*:6020",
                    input_json_path="input/events_2.json"
                )
            ),
            # Aggregator filter
            (
                FilterAggregator,
                FilterAggregatorConfig(
                    id="aggregator",
                    log_level="debug",
                    sources="tcp://127.0.0.1:6002;>cam_1, tcp://127.0.0.1:6020;>cam_2",
                    outputs="tcp://*:6004",
                    aggregations=aggregations,
                    forward_extra_fields=forward_extra_fields,
                    forward_image=forward_image,
                    append_op_to_key=append_op_to_key, 
                    forward_upstream_data=forward_upstream_data,
                    debug=debug,
                    mq_log="pretty" 
                )
            ),
            # Web visualization
            (
                Webvis,
                dict(
                    id="webvis",
                    sources="tcp://127.0.0.1:6004",
                    port=8002
                )
            )
        ]
    )

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    main()