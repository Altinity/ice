from pyiceberg.catalog import load_catalog
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("name", type=str, help="e.g. ns1.table1")

    args = parser.parse_args()

    catalog = load_catalog("default")
    name = args.name
    t = catalog.load_table(name)
    print(t.scan(limit=10).to_arrow())
