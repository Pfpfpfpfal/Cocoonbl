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
### Python lib gnn
```bash
py -3.11 -m venv gnn-env
```

```bash
gnn-env\Scripts\activate
```

### Python lib gnn
```bash
py -3.13 -m venv lgbm-env
```

```bash
lgbm-env\Scripts\activate
```

Запуск сервисов

Права для файла


chmod +x docker/airflow/airflow-init.sh

Запуск Postgres


docker compose up -d postgres-meta

Инициализация пользователя и подключения


docker compose up airflow-init


Процесс должен завершиться с кодом 0, иначе ошибка.

Запуск сервиса AirFlow


docker compose up -d webserver scheduler


Доступ к web-сервису: http://localhost:8083