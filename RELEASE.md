# Changelog
Aggregator filter release notes

## [Unreleased]

### Changed

- Bump the openfilter dependency to 1.3.0

## v1.1.8 - 2026-08-10

### Changed

- Build the image on `openfilter-base` (weekly apt-upgraded python-slim) instead of a stale `python:X.Y.Z-slim` pin, clearing the OS-package CVEs the pin carried.
- Update the openfilter dependency to 1.2.2

## v1.1.7 - 2026-08-04

### Changed
- Update `openfilter[all]` to `>=1.2.1`.
- Grant `id-token: write` in `create-release.yaml` so the public release workflow can produce a keyless (cosign) SBOM attestation for the published image (once the shared SBOM steps land).
- Fix the `RELEASE.md` header (`# Changelog` first line; the stray `# v1.1.6` H1 broke the changelog-parser).
- Pin the Docker base to `python:3.11.12-slim` (was `python:3.11-slim`).
- Repair the corrupted `docker-compose.yaml` — a bad find/replace in #2 had glued `openfilter-aggregator:1.1.5` onto every colon-value (the `LOG_LEVEL` defaults, the volume mount, the `8001` port) and mangled the utility image tags. Reconstruct from the pre-corruption revision, point the utility images at `containers.openfilter.io/plainsightai/openfilter-{video-in,webvis}:1.2.1`, and pin the filter's own image to the release version `openfilter-aggregator:1.1.7`.
- Update dev-tooling floors (`setuptools>=83.0.0`) and switch dev pins to range pins.

## v1.1.6 - 2026-04-23

### Changed
- Update the openfilter dependency to `>=0.1.30`, and align the CI workflow with the shared release gate (source-paths).
- Fix release workflow secret names: `PYPI_API_TOKEN` → `PLAINSIGHT_PYPI_TOKEN`, `DOCKERHUB_TOKEN` → `DOCKERHUB_ACCESS_TOKEN` (org-level secret names). Without this the PyPI / Docker Hub tokens resolved to empty and no package has been published since the migration.

## v1.1.5 - 2026-04-20

### Changed
- Remove redundant ci.yaml (shared workflow handles PR testing)
- Add push + pull_request triggers to create-release.yaml

## v1.1.4 - 2026-04-15

### Changed
- Add CI/CD workflows: create-release.yaml (Docker Hub publishing), ci.yaml (PR testing), security-scan.yaml
- Update openfilter dependency to >=0.1.27

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
