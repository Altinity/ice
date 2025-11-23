# ice [![build](https://github.com/altinity/ice/actions/workflows/verify.yaml/badge.svg)](https://github.com/altinity/ice/actions/workflows/verify.yaml) [![status: experimental](https://img.shields.io/badge/status-experimental-orange.svg)]()

A suite of tools aimed at making [Iceberg](https://iceberg.apache.org/) REST Catalogs more approachable. 

- [ice-rest-catalog](ice-rest-catalog/) - A Kubernetes-ready Iceberg REST catalog backed by [etcd](https://etcd.io/).  
Run locally with a one-liner: `ice-rest-catalog -c config.yaml`.

- [ice](ice/) - A CLI for interacting with Iceberg REST catalogs.  
Create/delete tables, insert data with `ice insert -p ns1.table1 file://example.parquet`, etc. 

## Installation

Pre-built binaries (+ links to Docker images for [ice](https://hub.docker.com/r/altinity/ice) and [ice-rest-catalog](https://hub.docker.com/r/altinity/ice-rest-catalog)) are available from [GitHub Releases](https://github.com/Altinity/ice/releases) page.

**Two types of binaries are available:**

1. **Java-based binaries** - Require Java 21+ to run (available [here](https://adoptium.net/installation/))
2. **Native binaries** - Standalone executables with no Java dependency
   - `ice-native-amd64` - Static binary (musl) for x86_64 Linux (no dependencies)
   - `ice-native-arm64` - Dynamic binary for ARM64 Linux (requires glibc)

## Usage

See [examples/](examples/).

## Development

### Standard Build (Java-based)

Install [sdkman](https://sdkman.io/install), then

```shell
git clone https://github.com/altinity/ice && cd ice

# optional but recommended
cp .envrc.sample .envrc
direnv allow

# switch to java specified in .sdkmanrc (done automatically by direnv if `direnv allow`ed)
sdk env
  
# shows how to build, test, etc. project
./mvnw
```

### Native Image Build (Standalone)

Build standalone native binaries with no Java dependency:

```shell
# Install prerequisites
sdk env  # or ensure Java 21+ and GraalVM are available

# For amd64 (static with musl - no dependencies)
mvn -Pnative-amd64-static -pl ice clean package -Dmaven.test.skip=true
For arm64
mvn -Pnative-arm64 -pl ice clean package -Dmaven.test.skip=true

# Docker builds
docker build -f ice/Dockerfile.native-amd64-static -t ice-native:amd64 .

docker build -f ice/Dockerfile.native-arm64 -t ice-native:arm64 .

## License

Copyright (c) 2025, Altinity Inc and/or its affiliates. All rights reserved.  
`ice` is licensed under the Apache License 2.0.

See [LICENSE](./LICENSE) for more details.

## Commercial Support

`ice` is maintained by Altinity.  
Altinity offers a range of services related to ClickHouse and analytics applications on Kubernetes.

- [Official website](https://altinity.com/) - Get a high level overview of Altinity and our offerings.
- [Altinity.Cloud](https://altinity.com/cloud-database/) - Run ClickHouse in our cloud or yours.
- [Altinity Support](https://altinity.com/support/) - Get Enterprise-class support for ClickHouse.
- [Slack](https://altinity.com/slack) - Talk directly with ClickHouse users and Altinity devs.
- [Contact us](https://hubs.la/Q020sH3Z0) - Contact Altinity with your questions or issues.
- [Free consultation](https://hubs.la/Q020sHkv0) - Get a free consultation with a ClickHouse expert today.
 
