# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased](https://github.com/altinity/ice/compare/v0.3.0...master)

## [0.3.0](https://github.com/altinity/ice/compare/v0.2.0...v0.3.0)

### Added
- [S3 Table buckets](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-buckets.html) support.
- `ice scan`, `ice create-namespace', `ice delete-namespace' commands.
- Ability to use `ice` with catalogs serving self-signed certificates (via `caCrt` config option).

### Changed
- `debug*` images not require `java -jar` to launch ice/ice-rest-catalog.

### Fixed
- ice-rest-catalog retrying operations regardless of whether errors are retryable or not. 
- ice insert failing when both --force-table-auth and -p are set even if table already exists and -p is effectively no-op.
- ice insert not including failed input URL.
- LocalFileIO printing properties.
- ice insert failing to insert parquet files with `uint32` & `uint64` columns.  
NOTE: `uint64` is cast to int64, which may or may not be appropriate depending on the actual range of values.

## [0.2.0](https://github.com/altinity/ice/compare/v0.1.1...v0.2.0)

### Added
- Ability to specify `ice` config via `ICE_CONFIG_YAML` env var and `ice-rest-catalog` config via `ICE_REST_CATALOG_YAML`.

### Changed
- `ice --partition/--sort` to accept YAML and not just JSON.

## [0.1.1](https://github.com/altinity/ice/compare/v0.1.0...v0.1.1)

### Fixed
- Metrics calculation when using --partition.
