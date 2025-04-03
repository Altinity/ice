## Changelog

REPLACE_WITH_CHANGELOG

## Installation

As repo is currently private, either download manually or use [gh](https://cli.github.com/) as shown below.  
Docker images are public.

### ice

```sh
gh release download --repo altinity/ice REPLACE_WITH_TAG -p ice-REPLACE_WITH_VER \
  --output ice && chmod a+x ice && sudo mv ice /usr/local/bin/
```

#### Docker

<!-- TODO: @digest -->

- `altinity/ice:REPLACE_WITH_TAG`
- `altinity/ice:debug-REPLACE_WITH_TAG`

### ice-rest-catalog

```sh
gh release download --repo altinity/ice REPLACE_WITH_TAG -p ice-rest-catalog-REPLACE_WITH_VER \
  --output ice-rest-catalog && chmod a+x ice-rest-catalog && sudo mv ice-rest-catalog /usr/local/bin/
```

#### Docker

<!-- TODO: @digest -->

- `altinity/ice-rest-catalog:REPLACE_WITH_TAG`
- `altinity/ice-rest-catalog:debug-REPLACE_WITH_TAG`
- `altinity/ice-rest-catalog:debug-with-ice-REPLACE_WITH_TAG`
