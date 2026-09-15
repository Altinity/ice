"""Independent validation of an ice-created v3 table using PyIceberg.

Runs inside the pyiceberg test container (see scenarios/pyiceberg/Dockerfile).
Reads the table via the REST catalog and asserts:
  - format_version == 3
  - the payload column has the v3-only UnknownType
  - the table scans to zero rows (unknown columns are always null per spec)

Usage: python3 validate.py <table-id>   (e.g. test_v3_unknown.t1)
"""

import os
import sys

from pyiceberg.catalog import load_catalog
from pyiceberg.types import UnknownType


def main() -> int:
    table_id = sys.argv[1] if len(sys.argv) > 1 else "test_v3_unknown.t1"
    catalog_uri = os.environ.get("CATALOG_URI_INTERNAL", "http://catalog:5000")
    s3_endpoint = os.environ.get("S3_ENDPOINT_INTERNAL", "http://minio:9000")

    catalog = load_catalog(
        "rest",
        uri=catalog_uri,
        warehouse="s3://test-bucket/warehouse",
        **{
            "s3.endpoint": s3_endpoint,
            "s3.access-key-id": "minioadmin",
            "s3.secret-access-key": "minioadmin",
        },
    )
    table = catalog.load_table(table_id)

    format_version = table.metadata.format_version
    assert format_version == 3, f"expected format_version 3, got {format_version}"
    print(f"OK pyiceberg: format_version == {format_version}")

    field = table.schema().find_field("payload")
    assert isinstance(
        field.field_type, UnknownType
    ), f"expected UnknownType for payload, got {field.field_type}"
    print(f"OK pyiceberg: payload type is {field.field_type}")

    rows = table.scan().to_arrow()
    assert rows.num_rows == 0, f"expected 0 rows, got {rows.num_rows}"
    print("OK pyiceberg: scan returned 0 rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
