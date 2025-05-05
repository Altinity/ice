import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog
import localcache
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("name", type=str, help="e.g. ns1.table1")
    parser.add_argument("--location", type=str, help="e.g. s3://bucket1/ns1/table1")
    parser.add_argument(
        "--schema-from-parquet",
        type=str,
        required=True,
        help="e.g. https://.../foo.parquet",
    )

    args = parser.parse_args()

    catalog = load_catalog("default")
    df = pq.read_table(localcache.download(args.schema_from_parquet))

    name = args.name
    namespace = name.split(".", 2)[0]
    catalog.create_namespace_if_not_exists(namespace)

    # not using create_table_if_not_exists because it uploads new metadata.json even if table already exists
    if not catalog.table_exists(name):
        catalog.create_table(
            name,
            schema=df.schema,
            location=args.location,
        )
