# examples/eks

In the example below, we:

- create a new EKS cluster + s3 bucket
- deploy `ice-rest-catalog` backed by etcd
- insert data via `ice`
- query data using clickhouse

```shell
# open shell containing `eksctl`, `kubectl`, `aws` (awscli2), `envsubst`, `clickhouse` + optional etcdctl
devbox shell

export CATALOG_BUCKET="$USER-ice-rest-catalog-demo"
export AWS_REGION=us-west-1

source aws.credentials

# create eks cluster
cat eks.envsubst.yaml | envsubst -no-unset -no-empty > eks.yaml
eksctl create cluster -f eks.yaml --kubeconfig=./kubeconfig
# eksctl update cluster -f eks.yaml

# create bucket for ice-rest-catalog
aws s3api create-bucket --bucket "$CATALOG_BUCKET" \
    --create-bucket-configuration "LocationConstraint=$AWS_REGION"

# deploy etcd
KUBECONFIG=./kubeconfig kubectl -n ice apply -f etcd.eks.yaml
# KUBECONFIG=./kubeconfig kubectl -n ice logs pods/ice-rest-catalog-etcd-0

# deploy ice-rest-catalog
cat ice-rest-catalog.eks.envsubst.yaml | envsubst -no-unset -no-empty > ice-rest-catalog.eks.yaml
KUBECONFIG=./kubeconfig kubectl -n ice apply -f ice-rest-catalog.eks.yaml
# KUBECONFIG=./kubeconfig kubectl -n ice logs pods/ice-rest-catalog-0

# make ice-rest-catalog reachable via http://localhost:5000
KUBECONFIG=./kubeconfig kubectl -n ice port-forward svc/ice-rest-catalog 5000:5000

# put some data into catalog
ice insert nyc.taxis -p \
  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet

# query
clickhouse local -q $"
SET allow_experimental_database_iceberg = 1;

-- (re)create iceberg db  
DROP DATABASE IF EXISTS ice;  

CREATE DATABASE ice
  ENGINE = DataLakeCatalog('http://localhost:5000')
  SETTINGS catalog_type = 'rest',
    auth_header = 'Authorization: Bearer foo', 
    warehouse = 's3://${CATALOG_BUCKET}';

select count(*) from ice.\`nyc.taxis\`;
"

# clean up
KUBECONFIG=./kubeconfig kubectl -n ice delete -f ice-rest-catalog.eks.yaml
KUBECONFIG=./kubeconfig kubectl -n ice delete -f etcd.eks.yaml
KUBECONFIG=./kubeconfig kubectl -n ice delete \
  pvc/data-ice-rest-catalog-0 pvc/data-ice-rest-catalog-etcd-0 # do no leak pvs

# delete bucket
aws s3 rb "s3://$CATALOG_BUCKET" --force

# delete eks cluster
eksctl delete cluster -f eks.yaml --disable-nodegroup-eviction
```

### Troubleshooting

1. `envsubst: invalid option -- 'n'`  

Solution: `envsubst`, from `gettext-base` package, lacks `-no-unset -no-empty` support. Install https://github.com/a8m/envsubst or 
use [devbox](https://www.jetify.com/docs/devbox/installing_devbox/) as shown above.   

2. `ice-rest-catalog` not starting.  

Solution: check pod status/logs 

```shell
KUBECONFIG=./kubeconfig kubectl -n ice describe pods/ice-rest-catalog-0
KUBECONFIG=./kubeconfig kubectl -n ice logs pods/ice-rest-catalog-0
```

3. `Code: 336. DB::Exception: Unknown database engine: DataLakeCatalog. (UNKNOWN_DATABASE_ENGINE)`

Solution: Make sure `clickhouse --version` is >=25.4.1.2514 or   
use [devbox](https://www.jetify.com/docs/devbox/installing_devbox/) as shown above.   
