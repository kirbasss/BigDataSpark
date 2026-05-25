SELECT count(*) FROM report_product_sales;
SELECT count(*) FROM report_customer_sales;
SELECT count(*) FROM report_time_sales;
SELECT count(*) FROM report_store_sales;
SELECT count(*) FROM report_supplier_sales;
SELECT count(*) FROM report_product_quality;


SELECT *
FROM report_product_sales
ORDER BY total_revenue DESC
LIMIT 10;

SELECT *
FROM report_customer_sales
ORDER BY total_spent DESC
LIMIT 10;

SELECT *
FROM report_time_sales
ORDER BY year_num, month_num
LIMIT 12;

SELECT *
FROM report_store_sales
ORDER BY total_revenue DESC
LIMIT 5;

SELECT *
FROM report_supplier_sales
ORDER BY total_revenue DESC
LIMIT 5;

SELECT *
FROM report_product_quality
ORDER BY product_rating DESC, total_quantity_sold DESC
LIMIT 10;