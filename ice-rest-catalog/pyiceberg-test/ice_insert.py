import sys
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog
import localcache
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("name", type=str, help="e.g. ns1.table1")
    parser.add_argument("url", type=str, nargs="+", help="e.g. https://.../foo.parquet")

    args = parser.parse_args()

    catalog = load_catalog("default")
    name = args.name
    t = catalog.load_table(name)

    for f in args.url:
        df = pq.read_table(localcache.download(f))
        t.append(df)
