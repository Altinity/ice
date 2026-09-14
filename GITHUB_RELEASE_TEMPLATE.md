## Installation

### ice

```sh
curl -sSL https://github.com/altinity/ice/releases/download/REPLACE_WITH_TAG/ice-REPLACE_WITH_VER \
  -o ice && chmod a+x ice && sudo mv ice /usr/local/bin/
```

#### Native (no Java required; needs glibc)

```sh
# linux amd64
curl -sSL https://github.com/altinity/ice/releases/download/REPLACE_WITH_TAG/ice-native-amd64-REPLACE_WITH_VER \
  -o ice && chmod a+x ice && sudo mv ice /usr/local/bin/
```

```sh
# linux arm64
curl -sSL https://github.com/altinity/ice/releases/download/REPLACE_WITH_TAG/ice-native-arm64-REPLACE_WITH_VER \
  -o ice && chmod a+x ice && sudo mv ice /usr/local/bin/
```

#### Docker

<!-- TODO: @digest -->

- `altinity/ice:REPLACE_WITH_VER`
- `altinity/ice:debug-REPLACE_WITH_VER`

> `debug-*` images contain busybox shell.

### ice-rest-catalog

```sh
curl -sSL https://github.com/altinity/ice/releases/download/REPLACE_WITH_TAG/ice-rest-catalog-REPLACE_WITH_VER \
  -o ice-rest-catalog && chmod a+x ice-rest-catalog && sudo mv ice-rest-catalog /usr/local/bin/
```

#### Docker

<!-- TODO: @digest -->

- `altinity/ice-rest-catalog:REPLACE_WITH_VER`
- `altinity/ice-rest-catalog:debug-REPLACE_WITH_VER`
- `altinity/ice-rest-catalog:debug-with-ice-REPLACE_WITH_VER`

> `debug-*` images contain busybox shell.
