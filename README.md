# ice [![build](https://github.com/altinity/ice/actions/workflows/verify.yaml/badge.svg)](https://github.com/altinity/ice/actions/workflows/verify.yaml)

A suite of tools aimed at making [Iceberg](https://iceberg.apache.org/) REST Catalogs more approachable. 

- [ice-rest-catalog](ice-rest-catalog/) - A Kubernetes-ready Iceberg REST catalog backed by [etcd](https://etcd.io/).  
Run locally with a one-liner: `ice-rest-catalog -c config.yaml`.

- [ice](ice/) - A CLI for interacting with Iceberg REST catalogs.  
Create/delete tables, insert data with `ice insert -p ns1.table1 file://example.parquet`, etc. 

## Demo

> `ice-rest-catalog` as Iceberg REST Catalog + `ice` to insert data + `clickhouse` to query

[![demo](https://github.com/user-attachments/assets/e3cfdb48-e7f0-4893-abe8-736c9655811d)](examples/scratch/)

## Installation

Pre-built binaries\* (+ links to Docker images for [ice](https://hub.docker.com/r/altinity/ice) and [ice-rest-catalog](https://hub.docker.com/r/altinity/ice-rest-catalog)) are available form [GitHub Releases](https://github.com/Altinity/ice/releases) page.
> \* currently require `java` 21+ to run (available [here](https://adoptium.net/installation/)).  

## Usage

See [examples/](examples/).

## Development

Install [sdkman](https://sdkman.io/install), then

```shell
git clone https://github.com/altinity/ice && cd ice

# switch to java specified in .sdkmanrc (done automatically by direnv when `allow`ed)
sdk env
  
# shows how to build, test, etc. project
./mvnw
```

## License

Copyright (c) 2025, Altinity Inc and/or its affiliates. All rights reserved.
`ice` is licensed under the Apache License 2.0.

See [LICENSE](./LICENSE) for more details.

## Commercial Support

- [Official website](https://altinity.com/) - Get a high level overview of Altinity and our offerings.
- [Altinity.Cloud](https://altinity.com/cloud-database/) - Run ClickHouse in our cloud or yours.
- [Altinity Support](https://altinity.com/support/) - Get Enterprise-class support for ClickHouse.
- [Slack](https://altinity.com/slack) - Talk directly with ClickHouse users and Altinity devs.
- [Contact us](https://hubs.la/Q020sH3Z0) - Contact Altinity with your questions or issues.
- [Free consultation](https://hubs.la/Q020sHkv0) - Get a free consultation with a ClickHouse expert today.
 
