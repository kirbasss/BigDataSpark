from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, avg, count, desc, round as _round
)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


def main():
    spark = (
        SparkSession.builder
        .appName("Build ClickHouse Reports")
        .getOrCreate()
    )

    postgres_url = "jdbc:postgresql://postgres:5432/bigdata_lab2"
    postgres_props = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }

    clickhouse_url = "jdbc:clickhouse://clickhouse:8123/bigdata_lab2"
    clickhouse_props = {
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
        "user": "default",
        "password": "default"
    }

    # ---------------------------------------------------------
    # 1. Read star schema from PostgreSQL
    # ---------------------------------------------------------
    fact_sales = spark.read.jdbc(postgres_url, "fact_sales", properties=postgres_props)
    dim_customer = spark.read.jdbc(postgres_url, "dim_customer", properties=postgres_props)
    dim_seller = spark.read.jdbc(postgres_url, "dim_seller", properties=postgres_props)
    dim_store = spark.read.jdbc(postgres_url, "dim_store", properties=postgres_props)
    dim_supplier = spark.read.jdbc(postgres_url, "dim_supplier", properties=postgres_props)
    dim_product = spark.read.jdbc(postgres_url, "dim_product", properties=postgres_props)
    dim_date = spark.read.jdbc(postgres_url, "dim_date", properties=postgres_props)

    # ---------------------------------------------------------
    # 2. Unified dataframe for analytics
    # ---------------------------------------------------------
    sales_df = (
        fact_sales.alias("f")
        .join(dim_customer.alias("c"), col("f.customer_id") == col("c.customer_id"), "inner")
        .join(dim_seller.alias("se"), col("f.seller_id") == col("se.seller_id"), "inner")
        .join(dim_store.alias("st"), col("f.store_id") == col("st.store_id"), "inner")
        .join(dim_product.alias("p"), col("f.product_id") == col("p.product_id"), "inner")
        .join(dim_supplier.alias("su"), col("p.supplier_id") == col("su.supplier_id"), "left")
        .join(dim_date.alias("d"), col("f.date_id") == col("d.date_id"), "inner")
    )

    # ---------------------------------------------------------
    # 3. Report 1: product sales
    # ---------------------------------------------------------
    report_product_sales = (
        sales_df
        .groupBy(
            col("p.product_id"),
            col("p.product_name"),
            col("p.product_category"),
            col("p.product_rating"),
            col("p.product_reviews")
        )
        .agg(
            _sum("f.sale_quantity").alias("total_quantity_sold"),
            _sum("f.sale_total_price").alias("total_revenue"),
            avg("p.product_rating").alias("avg_rating"),
            avg("p.product_reviews").alias("avg_reviews")
        )
        .withColumn("avg_rating", _round(col("avg_rating"), 2))
        .withColumn("avg_reviews", _round(col("avg_reviews"), 2))
    )

    # ---------------------------------------------------------
    # 4. Report 2: customer sales
    # ---------------------------------------------------------
    report_customer_sales = (
        sales_df
        .groupBy(
            col("c.customer_id"),
            col("c.first_name"),
            col("c.last_name"),
            col("c.country")
        )
        .agg(
            count("f.fact_sale_id").alias("total_orders"),
            _sum("f.sale_total_price").alias("total_spent"),
            avg("f.sale_total_price").alias("avg_check")
        )
        .withColumn("avg_check", _round(col("avg_check"), 2))
    )

    # ---------------------------------------------------------
    # 5. Report 3: time sales
    # ---------------------------------------------------------
    report_time_sales = (
        sales_df
        .groupBy(
            col("d.year_num"),
            col("d.month_num"),
            col("d.month_name")
        )
        .agg(
            count("f.fact_sale_id").alias("total_orders"),
            _sum("f.sale_quantity").alias("total_quantity"),
            _sum("f.sale_total_price").alias("total_revenue"),
            avg("f.sale_total_price").alias("avg_order_value")
        )
        .withColumn("avg_order_value", _round(col("avg_order_value"), 2))
        .orderBy("year_num", "month_num")
    )

    # ---------------------------------------------------------
    # 6. Report 4: store sales
    # ---------------------------------------------------------
    report_store_sales = (
        sales_df
        .groupBy(
            col("st.store_id"),
            col("st.store_name"),
            col("st.city"),
            col("st.country")
        )
        .agg(
            count("f.fact_sale_id").alias("total_orders"),
            _sum("f.sale_total_price").alias("total_revenue"),
            avg("f.sale_total_price").alias("avg_check")
        )
        .withColumn("avg_check", _round(col("avg_check"), 2))
    )

    # ---------------------------------------------------------
    # 7. Report 5: supplier sales
    # ---------------------------------------------------------
    report_supplier_sales = (
        sales_df
        .groupBy(
            col("su.supplier_id"),
            col("su.supplier_name"),
            col("su.supplier_country")
        )
        .agg(
            _sum("f.sale_total_price").alias("total_revenue"),
            avg("p.product_price").alias("avg_product_price"),
            _sum("f.sale_quantity").alias("total_quantity_sold")
        )
        .withColumn("avg_product_price", _round(col("avg_product_price"), 2))
    )

    # ---------------------------------------------------------
    # 8. Report 6: product quality
    # ---------------------------------------------------------
    report_product_quality = (
        sales_df
        .groupBy(
            col("p.product_id"),
            col("p.product_name"),
            col("p.product_rating"),
            col("p.product_reviews")
        )
        .agg(
            _sum("f.sale_quantity").alias("total_quantity_sold"),
            _sum("f.sale_total_price").alias("total_revenue")
        )
    )

    # ---------------------------------------------------------
    # 9. Save reports to ClickHouse
    # ---------------------------------------------------------
    report_product_sales.write.jdbc(
        url=clickhouse_url,
        table="report_product_sales",
        mode="overwrite",
        properties=clickhouse_props
    )

    report_customer_sales.write.jdbc(
        url=clickhouse_url,
        table="report_customer_sales",
        mode="overwrite",
        properties=clickhouse_props
    )

    report_time_sales.write.jdbc(
        url=clickhouse_url,
        table="report_time_sales",
        mode="overwrite",
        properties=clickhouse_props
    )

    report_store_sales.write.jdbc(
        url=clickhouse_url,
        table="report_store_sales",
        mode="overwrite",
        properties=clickhouse_props
    )

    report_supplier_sales.write.jdbc(
        url=clickhouse_url,
        table="report_supplier_sales",
        mode="overwrite",
        properties=clickhouse_props
    )

    report_product_quality.write.jdbc(
        url=clickhouse_url,
        table="report_product_quality",
        mode="overwrite",
        properties=clickhouse_props
    )

    spark.stop()


if __name__ == "__main__":
    main()