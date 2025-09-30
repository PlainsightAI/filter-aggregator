import logging, json, time
from openfilter.filter_runtime import Filter, FilterConfig
from openfilter.filter_runtime.filter import Frame

class EchoFilterConfig(FilterConfig):
    """Configuration for the EchoFilter."""
    input_json_path: str = "input/events.json"  # Path to the JSON file to read

class EchoFilter(Filter):
    """A filter that reads a JSON file and cycles through its contents."""
    
    def setup(self, config: EchoFilterConfig):
        """Initialize the filter with the given configuration."""
        self.cfg = config
        self.current_index = 0
        self.events = self._load_json_file()
        logging.info(f"EchoFilter initialized with {len(self.events)} events")

    def _load_json_file(self) -> list:
        """Load and parse the JSON file."""
        try:
            with open(self.cfg.input_json_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Failed to load JSON file: {e}")
            return []

    def process(self, frames: dict) -> dict:
        """Process incoming frames and echo JSON data.
        
        Args:
            frames: Dictionary of topic -> Frame mappings
            
        Returns:
            Dictionary with a single 'main' topic containing the current JSON event
        """
        if not self.events:
            return {**frames}

        # Get the current event
        event = self.events[self.current_index]
        
        # Move to next event, cycle back to start if at end
        self.current_index = (self.current_index + 1) % len(self.events)
        
        # Create a new frame with the event data
        if frames:
            exemplar = next(iter(frames.values()))
            new_frame = exemplar.copy()
            if 'meta' not in new_frame.data:
                new_frame.data['meta'] = {}
            new_frame.data['meta'].update(event)
        else:
            new_frame = Frame(data=event)
            
        # Sleep for 1 second
        time.sleep(1)
        
        # Return the frame in the 'main' topic
        return {"main": new_frame}

    def shutdown(self):
        """Clean up resources."""
        logging.info("EchoFilter shutting down")

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    EchoFilter.run() 