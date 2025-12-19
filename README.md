# Cocoonbl
## Description
**Схема**

В папке data хранятся csv(4 [файла](https://www.kaggle.com/code/pavan1512/ieee-cis-fraud-detection))
## Cluster Run
Запуск кластера Lakehouse + Spark + Trino + Airflow
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
### Airflow
```bash
docker compose -f airflow.yml up -d airflow-init
```
Работа контейнера должна завершиться с кодом `0`, иначе ошибка инициализации

```bash
docker compose -f airflow.yml up -d scheduler webserver
```
## Data server
Поднять сервер для скачивания данных
```bash
cd data
```

```bash
python -m http.server 8000
```
## Dashboard
**Подготовка к запуску dashboard**
```bash
go mod init antifraud-dashboard
```

```bash
go mod tidy
```

```bash
go get github.com/trinodb/trino-go-client
```
**Запуск dashboard**
```bash
go run main.go
```
## Python
Библиотеки для обучения и запуска моделей моделей находятся в `requirements_modelname.txt`
### Python lib gnn
```bash
py -3.11 -m venv gnn-env
```

```bash
gnn-env\Scripts\activate
```

```bash
pip install -r requirements_gnn.txt
```
### Python lib lgbm
```bash
py -3.13 -m venv lgbm-env
```

```bash
lgbm-env\Scripts\activate
```

```bash
pip install -r requirements_lgbm.txt
```

## Описание Dag-ов
**init_namespaces** - создание слоев `raw`, `cleaned`, `features`, `marts`, `graph` в S3
**csv_to_raw** - кладет файлы csv в S3 в формате `parquet`
