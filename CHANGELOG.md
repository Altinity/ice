# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased](https://github.com/altinity/ice/compare/v0.8.0...master)

## [0.8.0](https://github.com/altinity/ice/compare/v0.7.3...v0.8.0)

### Added
- ice: Support for using `ice --copy-object` on files over 5 GiB in size.  

### Fixed
- ice --watch not url decoding object keys from SQS messages.
- ice: metrics generated for time/timestamp* columns with millisecond precision ([#61](https://github.com/Altinity/ice/pull/61)).
- ice: NPE while evaluating s3 wildcard expressions against minio ([#60](https://github.com/Altinity/ice/pull/60)).

## [0.7.3](https://github.com/altinity/ice/compare/v0.7.2...v0.7.3)

### Fixed
- ice-rest-catalog: ORPHAN_CLEANUP maintenance job failing after snapshot cleanup.

## [0.7.2](https://github.com/altinity/ice/compare/v0.7.1...v0.7.2)

### Fixed
- ice failing to ignore deleted files with % in their names.

## [0.7.1](https://github.com/altinity/ice/compare/v0.7.0...v0.7.1)

### Fixed
- ice: ClassCastException when reading metrics for enum columns. 

## [0.7.0](https://github.com/altinity/ice/compare/v0.6.2...v0.7.0)

### Changed
- ice --watch to ignore deleted files.
- ice --watch to recreate table if deleted and --create-table/-p is set.
- org.apache.parquet.CorruptStatistics warnings to exclude stacktrace.

## Fixed
- ice YAML/JSON inputs parsing ignoring trailing tokens.

### Removed
- jvm stats logging.

## [0.6.2](https://github.com/altinity/ice/compare/v0.6.1...v0.6.2)

### Fixed
- ice: `watch` ignoring all `s3:ObjectCreated:*` but `s3:ObjectCreated:Put`.

## [0.6.1](https://github.com/altinity/ice/compare/v0.6.0...v0.6.1)

### Fixed
- ice: sort order check.

## [0.6.0](https://github.com/altinity/ice/compare/v0.5.1...v0.6.0)

### Added
- ice: Support for `warehouse=file:///absolute/path`.
- ice: Ability to whitelist `file://` locations outside the `warehouse=file://...` (useful when referencing 
Parquet files located elsewhere on the filesystem).

## [0.5.1](https://github.com/altinity/ice/compare/v0.5.0...v0.5.1)

### Fixed
- ice: `ice delete --dry-run`.

## [0.5.0](https://github.com/altinity/ice/compare/v0.4.0...v0.5.0)

### Added
- ice: Support for `ice insert` in watch mode. See [examples/s3watch](examples/s3watch) for details.
- ice-rest-catalog: `MANIFEST_COMPACTION`, `DATA_COMPACTION`, `SNAPSHOT_CLEANUP` and `ORPHAN_CLEANUP` maintenance routines. These
can be enabled either via ice-rest-catalog.yaml/maintenance section or performed ad-hoc via `ice-rest-catalog perform-maintenance`. 
- ice: `ice delete` command.

### Changed
- ice: `ice delete-table` not to delete any data unless `--purge` is specified.
- ice-rest-catalog: catalog maintenance config section. `snapshotTTLInDays` moved to `maintenance.snapshotTTLInDays`.

### Fixed
- ice: Partitioning metadata missing when data is inserted with `--no-copy` or `--s3-copy-object`.
- ice: `NULLS_FIRST`/`NULLS_LAST` being ignored when sorting.
- ice: Path construction in `localfileio`.  

## [0.4.0](https://github.com/altinity/ice/compare/v0.3.1...v0.4.0)

### Added
- `name` (catalog name) config option for ice-rest-catalog that enables multiple catalogs share the same etcd/postgresql/etc. instance.

### Fixed
- `ice scan` failing on a table with `timestamptz` column(s).
- ice insert: `date` type handling.

## [0.3.1](https://github.com/altinity/ice/compare/v0.3.0...v0.3.1)

### Fixed
- ice-rest-catalog: s3tables auth.

## [0.3.0](https://github.com/altinity/ice/compare/v0.2.0...v0.3.0)

### Added
- [S3 Table buckets](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-buckets.html) support.
- `ice scan`, `ice create-namespace`, `ice delete-namespace` commands.
- Ability to use `ice` with catalogs serving self-signed certificates (via `caCrt` config option).

### Changed
- `debug*` images to not require `java -jar` to launch ice/ice-rest-catalog.

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
