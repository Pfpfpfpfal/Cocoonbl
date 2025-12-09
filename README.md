# Cocoonbl
## Description
В папке data хранятся csv(4 файла) (https://www.kaggle.com/code/pavan1512/ieee-cis-fraud-detection)
## GO
```bash
go mod init antifraud-dashboard
```

```bash
go mod tidy
```

```bash
go get github.com/trinodb/trino-go-client
```
## Cluster Run
### Create Network
```bash
docker network create coc-net || true
```
### Lakehouse

```bash
docker compose -f lakehouse.yml up -d
```
### Spark

```bash
docker compose -f spark.yml up -d
```
### Trino
```bash
docker compose -f trino.yml up -d
```