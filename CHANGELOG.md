# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased](https://github.com/altinity/ice/compare/v0.2.0...master)

## [0.2.0](https://github.com/altinity/ice/compare/v0.1.1...v0.2.0)

### Added
- Ability to specify `ice` config via `ICE_CONFIG_YAML` env var and `ice-rest-catalog` config via `ICE_REST_CATALOG_YAML`.

### Changed
- `ice --partition/--sort` to accept YAML and not just JSON.

## [0.1.1](https://github.com/altinity/ice/compare/v0.1.0...v0.1.1)

### Fixed
- Metrics calculation when using --partition.
