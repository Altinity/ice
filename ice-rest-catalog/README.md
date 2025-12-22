# ice-rest-catalog

A dead-simple Iceberg REST Catalog backed by [etcd](https://etcd.io/).

## Usage

Generally speaking, all you need to start your own instance of `ice-rest-catalog` is to 
create `.ice-rest-catalog.yaml` (schema defined [here](src/main/java/com/altinity/ice/rest/catalog/internal/config/Config.java)) 
and then execute `ice-rest-catalog`. 
That's it.

Examples of `.ice-rest-catalog.yaml` (as well as Kubernetes deployment manifests) can be found [here](../examples/). 

## Maintenance mode
The REST Catalog can also be run in maintenance mode to 
perform background cleanup or optimization tasks, such as expiring snapshots and removing orphan files, at scheduled intervals. Maintenance tasks are run automatically according to the cron-like schedule you provide (e.g., `"every 1 minutes"`).


```
ice-rest-catalog perform-maintenance --schedule "every 1 minutes"
```
