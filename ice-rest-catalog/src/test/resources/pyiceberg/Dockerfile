# PyIceberg test image for scenario ITs: independent Iceberg reader used to validate
# tables/data written by ice. Built at test time via Testcontainers ImageFromDockerfile.
FROM python:3.12-slim
ARG PYICEBERG_VERSION=0.12.0
RUN pip install --no-cache-dir "pyiceberg[pyarrow,s3fs]==${PYICEBERG_VERSION}"
