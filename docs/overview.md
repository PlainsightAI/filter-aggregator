---
title: Aggregator
sidebar_label: Overview
sidebar_position: 1
---

The **Aggregator** is a powerful filter that aggregates numeric and categorical data from upstream filters. It supports various aggregation operations and can work with multiple upstream producers and downstream consumers.

### Features

- **Multiple Aggregation Operations**
  - Sum, Average, Min, Max
  - Count and Count Distinct
  - Median, Standard Deviation
  - Any, All, Mode operations
  - Distinct value collection

- **Flexible Configuration**
  - Support for nested fields using dot notation
  - Optional forwarding of extra fields
  - Image forwarding capability
  - Source data forwarding option
  - Customizable output key naming

### Use Cases

- Aggregating metrics from multiple sources
- Computing statistics across multiple frames
- Collecting unique values from different sources
- Combining data from parallel processing pipelines
- Data normalization and consolidation

### Configuration

The Aggregator can be configured using environment variables or direct configuration:

```python
# Environment Variables
FILTER_AGGREGATIONS='{"meta.sheeps":"sum", "meta.door_time":"avg", "meta.states":"distinct"}'
FILTER_FORWARD_EXTRA_FIELDS=true
FILTER_FORWARD_IMAGE=false
FILTER_APPEND_OP_TO_KEY=true
FILTER_FORWARD_UPSTREAM_DATA=true
FILTER_DEBUG=false
```

Or using Python configuration:

```python
config = FilterAggregatorConfig(
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
```

### Configuration Combinations

Here are examples of different configuration combinations and their effects:

#### 1. Basic Aggregation with Extra Fields
```python
config = FilterAggregatorConfig(
    aggregations={"meta.count": "sum"},
    forward_extra_fields=True,  # Default
    append_op_to_key=True,      # Default
    forward_image=False,        # Default
    forward_upstream_data=True   # Default
)

# Input
frames = {
    "source1": Frame(
        image=None,
        data={
            "meta": {"count": 5},
            "extra": {"info": "source1"}
        }
    ),
    "source2": Frame(
        image=None,
        data={
            "meta": {"count": 3},
            "extra": {"info": "source2"}
        }
    )
}

# Output
{
    "meta": {"count_sum": 8},
    "extra": {"info": "source1"}  # Copied from first frame
}
```

#### 2. Image Forwarding with Operation Suffix
```python
config = FilterAggregatorConfig(
    aggregations={"meta.count": "sum"},
    forward_image=True,
    append_op_to_key=True
)

# Input
frames = {
    "source1": Frame(
        image=image1,  # Some image data
        data={"meta": {"count": 5}},
        format="RGB"
    ),
    "source2": Frame(
        image=image2,  # Different image data
        data={"meta": {"count": 3}},
        format="RGB"
    )
}

# Output
{
    "meta": {"count_sum": 8},
    "image": image1,  # Image from first frame
    "format": "RGB"
}
```

#### 3. Upstream Data Forwarding
```python
config = FilterAggregatorConfig(
    aggregations={"meta.count": "sum"},
    forward_upstream_data=True
)

# Input
frames = {
    "source1": Frame(
        image=None,
        data={"meta": {"count": 5}}
    ),
    "source2": Frame(
        image=None,
        data={"meta": {"count": 3}}
    )
}

# Output
{
    "main": Frame(
        image=None,
        data={"meta": {"count_sum": 8}}
    ),
    "source1": Frame(
        image=None,
        data={"meta": {"count": 5}}
    ),
    "source2": Frame(
        image=None,
        data={"meta": {"count": 3}}
    )
}
```

#### 4. No Operation Suffix
```python
config = FilterAggregatorConfig(
    aggregations={"meta.count": "sum"},
    append_op_to_key=False
)

# Input
frames = {
    "source1": Frame(
        image=None,
        data={"meta": {"count": 5}}
    ),
    "source2": Frame(
        image=None,
        data={"meta": {"count": 3}}
    )
}

# Output
{
    "meta": {"count": 8}  # Note: no "_sum" suffix
}
```

#### 5. Complete Configuration Example
```python
config = FilterAggregatorConfig(
    aggregations={
        "meta.count": "sum",
        "meta.temperature": "avg",
        "meta.status": "distinct"
    },
    forward_extra_fields=True,
    forward_image=True,
    append_op_to_key=True,
    forward_upstream_data=True
)

# Input
frames = {
    "source1": Frame(
        image=image1,
        data={
            "meta": {
                "count": 5,
                "temperature": 20,
                "status": "active"
            },
            "extra": {"info": "source1"}
        },
        format="RGB"
    ),
    "source2": Frame(
        image=image2,
        data={
            "meta": {
                "count": 3,
                "temperature": 30,
                "status": "inactive"
            },
            "extra": {"info": "source2"}
        },
        format="RGB"
    )
}

# Output
{
    "main": Frame(
        image=image1,
        data={
            "meta": {
                "count_sum": 8,
                "temperature_avg": 25,
                "status_distinct": ["active", "inactive"]
            },
            "extra": {"info": "source1"}
        },
        format="RGB"
    ),
    "source1": Frame(
        image=image1,
        data={
            "meta": {
                "count": 5,
                "temperature": 20,
                "status": "active"
            },
            "extra": {"info": "source1"}
        },
        format="RGB"
    ),
    "source2": Frame(
        image=image2,
        data={
            "meta": {
                "count": 3,
                "temperature": 30,
                "status": "inactive"
            },
            "extra": {"info": "source2"}
        },
        format="RGB"
    )
}
```

### Configuration Tips

1. **forward_extra_fields**
   - Set to `True` when you want to preserve non-aggregated fields
   - Useful for maintaining metadata or context from the source frames
   - Only copies fields from the first frame

2. **forward_image**
   - Set to `True` when you need to preserve image data
   - Only forwards the image from the first frame
   - Useful when working with video or image processing pipelines

3. **append_op_to_key**
   - Set to `True` to make output keys more descriptive (e.g., "count_sum")
   - Set to `False` for cleaner output keys (e.g., just "count")
   - Helps distinguish between different aggregation operations on the same field

4. **forward_upstream_data**
   - Set to `True` to preserve original upstream frames
   - Useful for debugging or when downstream filters need access to source data
   - Forwards the original frames alongside the aggregated main frame
