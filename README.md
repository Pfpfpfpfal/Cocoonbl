# Cocoonbl
## Description

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