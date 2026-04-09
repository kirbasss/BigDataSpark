# BigDataSpark

Анализ больших данных - лабораторная работа №2 - ETL реализованный с помощью Spark

Выполнил Арусланов Кирилл, группа М8О-303Б-23

## Запуск контейнеров

```bash
docker-compose up -d
```

После запуска контейнеров PostgreSQL автоматически:
- создаёт таблицы (DDL)
- загружает данные из CSV в таблицу mock_data

Необходимо подождать около минуты.

## Запуск ETL задач (с Git Bash могут быть проблемы, необходимо добавить MSYS_NO_PATHCONV=1 перед командой)

### 1. ETL: mock_data -> звезда в PostgreSQL

```bash
docker exec -it bigdata_lab2_spark spark-submit --packages org.postgresql:postgresql:42.7.3 /opt/spark_jobs/etl_to_star.py
```

### 2. Построение отчётов в ClickHouse

```bash
docker exec -it bigdata_lab2_spark spark-submit --packages org.postgresql:postgresql:42.7.3,com.clickhouse:clickhouse-jdbc:0.9.8 /opt/spark_jobs/build_clickhouse_reports.py
```

## Проверка результатов (подключение в DBeaver)

### PostgreSQL
- **Host**: localhost
- **Port**: 5433
- **Database**: bigdata_lab2
- **User**: postgres
- **Password**: postgres

### ClickHouse
- **Host**: localhost
- **Port**: 8123 (HTTP) или 9000 (TCP)
- **Database**: bigdata_lab2
- **User**: default
- **Password**: default

Для проверки отчетов в ClickHouse можно выполнить скрипт sql_scripts/3_check_clickhouse.sql

## Остановка

```bash
docker-compose down -v
```
