# Changelog
Aggregator filter release notes

## [Unreleased]

## v1.1.3 - 2025-09-29
### Changed
- Updated documentation

## v1.1.0 - 2025-09-24
### Fixed
- Fixed `forward_upstream_data` functionality to properly forward all upstream frames (both with and without images)
- Corrected configuration parameter name from `forward_source_data` to `forward_upstream_data` in documentation

### Changed
- Updated documentation to reflect correct behavior and configuration parameters
- Improved README.md with comprehensive usage instructions and Mermaid pipeline diagram
- Updated overview.md with accurate examples and configuration descriptions

## v1.0.1 - 2025-07-14
### Added
- Use Openfilter instead of filteruntime

## v1.0.0 - 2025-02-28

### Added
- Initial Release: new Aggregator filter
- Support for multiple aggregation operations:
  - Basic operations: sum, avg, min, max
  - Statistical operations: median, std, mode
  - Set operations: count, count_distinct, distinct
  - Boolean operations: any, all
- Flexible configuration options:
  - Support for nested fields using dot notation
  - Optional forwarding of extra fields
  - Image forwarding capability
  - Source data forwarding option
  - Customizable output key naming
- Environment variable configuration support
- Comprehensive error handling for invalid operations
- Automatic handling of missing fields
- Support for multiple upstream producers
- Support for multiple downstream consumers
- Detailed logging with debug mode
- Python configuration interface
- Unit tests for all major features
