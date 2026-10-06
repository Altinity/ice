"""Independent validation of ice-inserted data using PyIceberg.

Runs inside the pyiceberg test container (see scenarios/pyiceberg/Dockerfile).
Asserts that the number of rows readable via PyIceberg (REST catalog + S3)
matches the number of rows in the source parquet file, and that expected
columns are present.

Usage: python3 validate.py <table-id> <input-parquet-path>
"""

import os
import sys

import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog


def main() -> int:
    table_id = sys.argv[1]
    input_parquet = sys.argv[2]
    catalog_uri = os.environ.get("CATALOG_URI_INTERNAL", "http://catalog:5000")
    s3_endpoint = os.environ.get("S3_ENDPOINT_INTERNAL", "http://minio:9000")

    catalog = load_catalog(
        "rest",
        uri=catalog_uri,
        warehouse="s3://test-bucket/warehouse",
        **{
            # ice-rest-catalog test config uses anonymous access; without this, pyiceberg
            # sends "Authorization: Bearer None" (LegacyOAuth2AuthManager) -> 403 Invalid token
            "auth": {"type": "noop"},
            "s3.endpoint": s3_endpoint,
            "s3.access-key-id": "minioadmin",
            "s3.secret-access-key": "minioadmin",
        },
    )
    table = catalog.load_table(table_id)

    arrow = table.scan().to_arrow()
    expected_rows = pq.read_metadata(input_parquet).num_rows
    assert (
        arrow.num_rows == expected_rows
    ), f"expected {expected_rows} rows (from {input_parquet}), got {arrow.num_rows}"
    print(f"OK pyiceberg: row count {arrow.num_rows} matches source parquet")

    column_names = arrow.column_names
    assert any(
        "sepal" in name for name in column_names
    ), f"expected a 'sepal' column, got {column_names}"
    assert "variety" in column_names, f"expected 'variety' column, got {column_names}"
    print(f"OK pyiceberg: columns {column_names}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
