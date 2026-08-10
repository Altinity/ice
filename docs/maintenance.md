# Maintenance Configuration

`ice-rest-catalog` runs background maintenance jobs on a configurable schedule. Maintenance is
**disabled by default** — set `maintenanceSchedule` in `ice-rest-catalog.yaml` to enable it.

## Example

```yaml
# skedule format: https://github.com/shyiko/skedule
# empty or omitted = maintenance disabled (default)
maintenanceSchedule: "every day 02:00"

maintenance:
  jobs:
    - MANIFEST_COMPACTION
    - DATA_COMPACTION
    - SNAPSHOT_CLEANUP
    - ORPHAN_CLEANUP
  maxSnapshotAgeHours: 120
  minSnapshotsToKeep: 1
  targetFileSizeMB: 512
  minInputFiles: 5
  dataCompactionCandidateMinAgeInHours: 3
  orphanFileRetentionPeriodInDays: 3
  orphanWhitelist:
    - "*/metadata/*"
    - "*/data/*"
  dryRun: false
```

## Reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `maintenanceSchedule` | string | _(empty — disabled)_ | Schedule in [skedule](https://github.com/shyiko/skedule) format, e.g. `"every day 02:00"`, `"every 6 hours"`. Empty or omitted disables automatic maintenance. |
| `maintenance.jobs` | string[] | all four jobs | Which jobs to run. Omit to run all. Values: `MANIFEST_COMPACTION`, `DATA_COMPACTION`, `SNAPSHOT_CLEANUP`, `ORPHAN_CLEANUP`. |
| `maintenance.maxSnapshotAgeHours` | int | `120` (5 days) | Snapshots older than this are expired by `SNAPSHOT_CLEANUP`. |
| `maintenance.minSnapshotsToKeep` | int | `1` | Minimum number of snapshots to retain even if older than `maxSnapshotAgeHours`. |
| `maintenance.targetFileSizeMB` | int | `512` | Target file size for `DATA_COMPACTION`. Minimum `64`. |
| `maintenance.minInputFiles` | int | `5` | Minimum number of small files in a partition before `DATA_COMPACTION` triggers. |
| `maintenance.dataCompactionCandidateMinAgeInHours` | int | `3` | Files younger than this are not eligible for compaction. Set to `-1` to disable the age gate. |
| `maintenance.orphanFileRetentionPeriodInDays` | int | `3` | Orphan files older than this are deleted by `ORPHAN_CLEANUP`. Set to `-1` to disable. |
| `maintenance.orphanWhitelist` | string[] | `["*/metadata/*", "*/data/*"]` | Glob patterns for paths that `ORPHAN_CLEANUP` is allowed to scan. |
| `maintenance.dryRun` | bool | `false` | When `true`, log what would be done without applying changes. |

## Jobs

| Job | What it does |
|-----|-------------|
| `MANIFEST_COMPACTION` | Rewrites small manifest files into larger ones to speed up scan planning. |
| `DATA_COMPACTION` | Merges small data files into larger ones (up to `targetFileSizeMB`) to improve query performance. |
| `SNAPSHOT_CLEANUP` | Expires snapshots older than `maxSnapshotAgeHours` (keeping at least `minSnapshotsToKeep`) and deletes their unreferenced metadata and data files. |
| `ORPHAN_CLEANUP` | Deletes files under `orphanWhitelist` paths that are not referenced by any snapshot and are older than `orphanFileRetentionPeriodInDays`. |

## Tips

- Start with `dryRun: true` to preview what maintenance would do before enabling it for real.
- To run only specific jobs, list them explicitly in `jobs`. For example, to run only snapshot and orphan cleanup:
  ```yaml
  maintenance:
    jobs:
      - SNAPSHOT_CLEANUP
      - ORPHAN_CLEANUP
  ```
- The schedule applies globally to all tables in the catalog. Per-table maintenance configuration is not yet supported.
