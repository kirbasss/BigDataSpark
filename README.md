# BigDataSpark

Анализ больших данных - лабораторная работа №2 - ETL реализованный с помощью Spark

Выполнил: Савинов Никита Олегович, группа М8О-303Б-23

## Цель работы
Целью работы является построение ETL-пайплайна на Apache Spark, который:

* загружает исходные данные из CSV-файлов

* преобразует их в аналитическую модель «звезда» в PostgreSQL

* на основе модели «звезда» формирует 6 аналитических витрин

* загружает витрины в ClickHouse (колоночная OLAP-БД)

Ключевое требование лабораторной – взаимодействие Spark с базами данных выполняется напрямую через JDBC, без промежуточных Python‑драйверов.

## Подход к моделированию

### 1. Модель «звезда» в PostgreSQL
Таблица фактов
fact_sales – отражает событие продажи. Содержит:

* количество (sale_quantity)

* сумму (sale_total_price)

* внешние ключи на все измерения

Измерения

* dim_customers – покупатели

* dim_sellers – продавцы

* dim_products – товары

* dim_stores – магазины

* dim_suppliers – поставщики

* dim_dates – даты

Все измерения денормализованы (звезда), что ускоряет аналитические запросы.

### 2. Загрузка через Spark JDBC

* CSV-файлы читаются Spark с явно заданной схемой (StructType)

* Spark выполняет очистку, приведение типов, дедупликацию

* Запись в PostgreSQL и ClickHouse выполняется через .write.jdbc()

* JDBC-драйверы (PostgreSQL и ClickHouse) встроены в образ Spark

### 3. Аналитические витрины в ClickHouse

На основе звёздной схемы построены 6 витрин (отдельных таблиц) в ClickHouse:

| Витрина | Содержание |
|---------|-------------|
| `top_products_by_revenue` | топ‑10 продуктов по выручке, количеству продаж и рейтингу |
| `best_customers` | топ‑10 клиентов по сумме покупок, средний чек |
| `monthly_sales_trends` | месячные и годовые тренды продаж |
| `store_performance` | топ‑5 магазинов по выручке, средний чек |
| `supplier_analysis` | топ‑5 поставщиков по выручке, средняя цена товара |
| `product_rating_insights` | продукты с наивысшим и наинизшим рейтингом, корреляция с продажами |

Для витрины product_rating_insights дополнительно вычислен показатель revenue_per_review, а также выделены отдельные строки для максимального и минимального рейтинга – это полностью соответствует заданию.

## Инструкция по запуску

### Запустить контейнеры

```bash
docker-compose up -d
```

Контейнеры: PostgreSQL (postgres_lab), ClickHouse (ch_marts), Spark (spark_etl).
Все зависимости (JDBC-драйверы) уже собраны внутри образа Spark.

### Выполнить первичный ETL: CSV → PostgreSQL (звезда)

```bash
docker exec -it spark_etl python3 star_etl.py
```

Скрипт:

* считывает 10 файлов MOCK_DATA*.csv из папки data/

* создаёт таблицу mock_data и схему «звезда»

* загружает данные через Spark JDBC

* выводит количество записей в каждой таблице

### Выполнить вторичный ETL: PostgreSQL → ClickHouse (витрины)

```bash
docker exec -it spark_etl python3 clickhouse_etl.py
```

Скрипт:

* читает таблицы звезды из PostgreSQL через JDBC

* строит 6 аналитических витрин

* загружает витрины в ClickHouse через JDBC

### Подключиться к базам данных (например, через DBeaver)

PostgreSQL
* Host: `localhost`
* Port: `5433`
* Database: `bigdata_lab`
* User: `postgres`
* Password: `postgres`

ClickHouse
* Host: `localhost`
* HTTP Port: `8123`
* Database: `marts`
* User: `default`
* Password: `clickhouse`

### Остановка контейнеров
=======
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
>>>>>>> f10fc60553acf3cc91268a671278018715cdfed3

```bash
docker-compose down -v
```

## Результат
В результате выполнения работы:

* создано Docker-окружение с PostgreSQL, ClickHouse и Spark

* написан ETL-скрипт на PySpark для построения звезды в PostgreSQL

* написан ETL-скрипт для расчёта витрин и загрузки в ClickHouse

* все операции чтения/записи БД выполнены через JDBC (драйверы лежат в контейнере Spark)

* получены 6 заполненных аналитических витрин в ClickHouse

Проверка:

* mock_data и fact_sales содержат ровно 10000 строк (по числу строк в исходных CSV)

* каждая витрина в ClickHouse содержит данные без дубликатов, агрегаты рассчитаны верно

## Вывод
В ходе лабораторной работы освоен практический опыт построения ETL-пайплайнов на Apache Spark, интеграция Spark с реляционными (PostgreSQL) и колоночными (ClickHouse) базами данных через JDBC.

Продемонстрирована возможность Spark выступать в роли универсального движка для:

* чтения разнородных источников (CSV, JDBC)

* трансформации данных в модель «звезда»

* построения агрегированных витрин для OLAP-систем

Полученная архитектура может быть масштабирована на большие объёмы данных и использована в промышленных проектах аналитики.
