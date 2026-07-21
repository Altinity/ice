# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.18.0](https://github.com/Altinity/ice/compare/v0.17.0...master)

### Added

- `ice` - Add support for deleting partitions with logic operators other than equals ([#194](https://github.com/Altinity/ice/pull/194))
- `ice` - Add --purge option to delete command to physically delete data files ([#192](https://github.com/Altinity/ice/pull/192))

### Fixed

- `ice-rest-catalog` - Add error message that explains the empty URI scheme instead of a NPE ([#188](https://github.com/Altinity/ice/pull/188))
- `ice` - Fix unauthorized exception with ice due to missing auth header ([#196](https://github.com/Altinity/ice/pull/196))

## [0.17.0](https://github.com/Altinity/ice/compare/v0.16.0...v0.17.0)

### Added

- `ice` - Upgrade iceberg-java to 1.9.2 and support adding required columns ([#145](https://github.com/Altinity/ice/pull/145))
- `ice` - Add logic to add non-primitive types to alter table add ([#183](https://github.com/Altinity/ice/pull/183))

## [0.16.0](https://github.com/Altinity/ice/compare/v0.16.0...v0.15.0)

### Added

- `ice-rest-catalog` - Add logs to display catalog backend (etcd/sqlite) and timeout for etcd ([#167](https://github.com/Altinity/ice/pull/167))
- `ice` - Add list-snapshots command ([#163](https://github.com/Altinity/ice/pull/163))
- `ice` - Add logic to export and import etcd catalog keys as JSON ([#174](https://github.com/Altinity/ice/pull/174))

### Other

- `ice` - Make files command printing logic reusable ([#165](https://github.com/Altinity/ice/pull/165))
- `ice-rest-catalog` - Added docs on scaling from 1 node to 3. ([#169](https://github.com/Altinity/ice/pull/169))
`
## [0.15.0](https://github.com/Altinity/ice/compare/v0.15.0...v0.14.0)

### Added

- `ice`, `ice-rest-catalog` - Added logic to externalize commit retries in rest-catalog. Added metrics for commit retries
  in rest-catalog. Added logic in insert to handle retries by fetching newer metadata. 
  Added logic to add Commit Lock using etcd distributed locks
- 
## [0.14.0](https://github.com/Altinity/ice/compare/v0.14.0...v0.13.0)

### Added
- `ice` - Add support for truncate partitioning ([#143](https://github.com/Altinity/ice/pull/143))
- `ice` - Add list-tables command to show tables for a given namespace ([#150](https://github.com/Altinity/ice/pull/150))
- `ice` - Add describe-metadata command to print metadata/manifest information ([#140](https://github.com/Altinity/ice/pull/140))
- `ice` - Added support for add_column after/before/first ([#147](https://github.com/Altinity/ice/pull/147))

### Fixed
- `ice` - Add error message for wrong namespace/table in describe ([#154](https://github.com/Altinity/ice/pull/154))

## [0.13.0](https://github.com/altinity/ice/compare/v0.12.0...v0.13.0)
### Added
- `ice` - Added iceberg-gcp mvn dependencies for GCS support ([#99](https://github.com/Altinity/ice/pull/99))
- `ice` - Added describe-parquet command to print info of parquet files ([#65](https://github.com/Altinity/ice/pull/65))
- `ice` - Prevent empty namespace creation ([#105](https://github.com/Altinity/ice/pull/105))
- `ice-rest-catalog` - Iceberg metrics and Grafana dashboard ([#82](https://github.com/Altinity/ice/pull/82))
- `ice` - Ice cli partitions ([#114](https://github.com/Altinity/ice/pull/114))
- `ice` - Add option to pass partition name when creating partitions. ([#123](https://github.com/Altinity/ice/pull/123))
- `ice` - Ice cli files command ([#112](https://github.com/Altinity/ice/pull/112))
- `ice` - Add support for list-namespaces ([#131](https://github.com/Altinity/ice/pull/131))
- `ice` - Add shell mode for autocompletion of commands. ([#118](https://github.com/Altinity/ice/pull/118))
- `ice` - Configurable compression ([#115](https://github.com/Altinity/ice/pull/115))
- `ice` - Added logic to support bucket partitioning ([#97](https://github.com/Altinity/ice/pull/97))

### Fixed
- `ice` - Null pageToken ([#128](https://github.com/Altinity/ice/pull/128))
- `ice` - Fix insert inferred partition not writing to partition folder ([#120](https://github.com/Altinity/ice/pull/120))
- `ice` - Fix delete partition for transformed partition types ([#94](https://github.com/Altinity/ice/pull/94))

### Other
- Add integration tests using templates ([#75](https://github.com/Altinity/ice/pull/75))
- Add Docker integration tests ([#108](https://github.com/Altinity/ice/pull/108))
- Disable docker test in mvn verify ([#109](https://github.com/Altinity/ice/pull/109))
- Fix pre release ([#113](https://github.com/Altinity/ice/pull/113))
- Added test for schema evolution. ([#125](https://github.com/Altinity/ice/pull/125))
- Added fix for malformed warehouse file path ([#133](https://github.com/Altinity/ice/pull/133))

## [0.12.0](https://github.com/altinity/ice/compare/v0.11.1...v0.12.0)

### Added
- `ice` - Added drop partition field in alter table.

### Changed
- `ice` - Logic to throw exception when the user tries to use -force-no-copy for file(row group/block) 
with multiple partitions.
- `ice` - User friendly message in REST catalog initialization if server cannot be reached.

### Fixed
• `ice` - Parquet conversion to iceberg for long data type.

### Documentation
- Added High level k8s architecture diagram.
- Documented inserting multiple files (and transaction support).

## [0.11.0](https://github.com/altinity/ice/compare/v0.10.1...v0.11.0)

### Added
- ice `--files-from` & `--retry-list-exit-code` cli options. 

### Fixed
- TZ information getting lost when partitioning ([#85](https://github.com/Altinity/ice/issues/85)).

## [0.10.1](https://github.com/altinity/ice/compare/v0.10.0...v0.10.1)

### Fixed
- Data partitioning based on time/timestamp* columns with millisecond precision. 

## [0.10.0](https://github.com/altinity/ice/compare/v0.9.0...v0.10.0)

### Added
- `ice --insecure` CLI option.

### Changed
- `ice describe` output to include `error`(s) when table(s) cannot be read due to the internal server error(s).  

### Fixed
- ice-rest-catalog/etcd: dropTable failing when table files are missing/corrupted.

## [0.9.0](https://github.com/altinity/ice/compare/v0.8.1...v0.9.0)

### Added
- Support for vended credentials.  
This enables creating new tables from existing Parquet files located in S3 without having direct access to object storage.
e.g. `ice create-table foo.bar --use-vended-credentials --schema-from-parquet=s3://catalog-bucket/foo/bar/external-data/sample.parquet`. 
- `ice alter-table` command. 
- `ice insert --force-duplicates` CLI flag.
- `ice describe` output to include nested metrics.

### Changed
- `ice insert` to accept files with compatible schema: subset of table schema and/or with primitive type promotions listed in
[Schema Evolution](https://iceberg.apache.org/spec/#schema-evolution) (previously file schema had to match exactly).
- `ice describe` output to minimize quotes.

### Fixed
- Calculation of nested metrics.

## [0.8.1](https://github.com/altinity/ice/compare/v0.8.0...v0.8.1)

### Fixed
- ice: nullCount metric calculation.

## [0.8.0](https://github.com/altinity/ice/compare/v0.7.3...v0.8.0)

### Added
- ice: Support for using `ice --s3-copy-object` on files over 5 GiB in size.  

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
