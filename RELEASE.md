# Changelog
Aggregator filter release notes

## [Unreleased]

## v1.1.7 - 2026-08-04

### Changed
- Update `openfilter[all]` to `>=1.2.1`.
- Grant `id-token: write` in `create-release.yaml` so the public release workflow can produce a keyless (cosign) SBOM attestation for the published image (once the shared SBOM steps land).
- Fix the `RELEASE.md` header (`# Changelog` first line; the stray `# v1.1.6` H1 broke the changelog-parser).
- Pin the Docker base to `python:3.11.12-slim` (was `python:3.11-slim`).
- Fix the `docker-compose.yaml` utility images (were the malformed `openfilter-video-in`/`webvis` + `-aggregator` concatenation) and point them at `containers.openfilter.io/plainsightai/openfilter-{video-in,webvis}:1.2.1`.
- Update dev-tooling floors (`setuptools>=83.0.0`) and switch dev pins to range pins.

## v1.1.6 - 2026-04-23

### Changed
- Bump openfilter SDK, align CI workflow with shared release gate (source-paths)

- Fix release workflow secret names: `PYPI_API_TOKEN` → `PLAINSIGHT_PYPI_TOKEN`, `DOCKERHUB_TOKEN` → `DOCKERHUB_ACCESS_TOKEN` (org-level secret names). Without this the PyPI / Docker Hub tokens resolved to empty and no package has been published since the migration.
- Bump openfilter dependency to `>=0.1.30`.

# Changelog
Aggregator filter release notes

## [Unreleased]

### Changed

- Bump openfilter to 1.1.0
- Bump openfilter to 1.1.1
- Bump openfilter to 1.1.2
- Bump the openfilter dependency to 1.2.0
- Bump the openfilter dependency to 1.2.1

## v1.1.5 - 2026-04-20

### Changed
- Remove redundant ci.yaml (shared workflow handles PR testing)
- Add push + pull_request triggers to create-release.yaml


## v1.1.4 - 2026-04-15

### Changed
- Add CI/CD workflows: create-release.yaml (Docker Hub publishing), ci.yaml (PR testing), security-scan.yaml
- Bump openfilter dependency to >=0.1.27


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
