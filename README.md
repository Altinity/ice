# ice ![build](https://github.com/altinity/ice/actions/workflows/verify.yaml/badge.svg)

A suite of tools aiming at simplifying [Iceberg](https://iceberg.apache.org/)+[ClickHouse](https://clickhouse.com/) deployment

- [ice-rest-catalog](ice-rest-catalog/) - A dead-simple Iceberg REST catalog. 
- [ice](ice/) - A CLI for loading data into Iceberg REST catalogs  (like `ice-rest-catalog` :)).

## Installation

Pre-built binaries\* (+ links to Docker images for [ice](https://hub.docker.com/r/altinity/ice) and [ice-rest-catalog](https://hub.docker.com/r/altinity/ice-rest-catalog)) are available form [GitHub Releases](https://github.com/Altinity/ice/releases) page.

\* currently require `java` 21+ (available [here](https://adoptium.net/installation/)) to run.  
Alternatively, `docker run -it --rm --network=host -v $(pwd)/ice.yaml:/etc/ice/ice.yaml:ro altinity/ice --help`.

## Usage

See [examples/](examples/).

## Development

Install [sdkman](https://sdkman.io/install), then

```shell
git clone https://github.com/altinity/ice && cd ice

# switch to java specified in .sdkmanrc (done automatically by direnv when `direnv allow`ed)
sdk env
  
# shows how to build, test, etc. project
./mvnw
```
