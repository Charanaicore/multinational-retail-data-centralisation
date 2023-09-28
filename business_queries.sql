
-- Task 1. How many stores does the business have and in which countries?
SELECT
    country_code AS country, 
    COUNT(store_code) AS total_no_stores
FROM dim_store_details
WHERE country_code IN ('GB', 'DE', 'US')
GROUP BY country_code
ORDER BY total_no_stores DESC;


-- Task 2. Which locations currently have the most stores?
SELECT
    locality, 
    COUNT(store_code) AS total_no_stores
FROM dim_store_details
GROUP BY locality
HAVING COUNT(store_code) >= 10
ORDER BY total_no_stores DESC;


-- Task 3. Which months produce the average highest cost of sales typically?
SELECT
    ROUND(SUM(product_quantity * product_price)::numeric, 2) AS total_sales,
    month
FROM
    orders_table AS ot
INNER JOIN
    dim_date_times AS ddt ON ot.date_uuid = ddt.date_uuid
INNER JOIN
    dim_products AS dp ON ot.product_code = dp.product_code
GROUP BY month
ORDER BY total_sales DESC
LIMIT 6;


-- Task 4. How many sales are coming from online?
SELECT
    COUNT(date_uuid) AS number_of_sales,
    SUM(product_quantity) AS product_quantity_count,
    CASE store_type
        WHEN 'Web Portal' THEN 'Web'
        ELSE 'Offline'
    END AS location
FROM
    orders_table AS ot
INNER JOIN
    dim_store_details AS dst ON ot.store_code = dst.store_code
GROUP BY location
ORDER BY number_of_sales;


-- Task 5. What percentage of sales come through each type of store?
WITH all_sales_by_store AS (
    SELECT
        store_type,
        ROUND((product_price * product_quantity)::numeric, 2) AS sale
    FROM
        orders_table as ot
    INNER JOIN
        dim_store_details AS dst ON ot.store_code = dst.store_code
    INNER JOIN
        dim_products AS dp ON ot.product_code = dp.product_code
)
SELECT
    store_type,
    SUM(sale) AS total_sales,
    ROUND(SUM(sale) * 100 / SUM(SUM(sale)) OVER (), 2) AS "percentage_total(%)"
FROM
    all_sales_by_store
GROUP BY
    store_type
ORDER BY
    total_sales DESC;


-- Task 6. Which month in each year produced the highest cost of sales?
SELECT
    ROUND(SUM(product_price * product_quantity)::numeric, 2) AS total_sales,
    year,
    month
FROM
    orders_table as ot
INNER JOIN
    dim_date_times AS ddt ON ot.date_uuid = ddt.date_uuid
INNER JOIN
    dim_products AS dp ON ot.product_code = dp.product_code
GROUP BY
    year, month
ORDER BY
    total_sales DESC
LIMIT 10;


-- Task 7. What is our staff headcount?
SELECT
    SUM(staff_numbers) AS total_staff_numbers,
    country_code
FROM
    dim_store_details
WHERE
    country_code IN ('US', 'GB', 'DE')
GROUP BY
    country_code
ORDER BY
    total_staff_numbers;


-- Task 8. Which German store type is selling the most?
SELECT
    ROUND(SUM(product_price * product_quantity)::numeric, 2) AS total_sales,
    store_type,
    country_code
FROM
    orders_table AS ot
INNER JOIN
    dim_store_details AS dst ON ot.store_code = dst.store_code
INNER JOIN
    dim_products AS dp ON ot.product_code = dp.product_code
WHERE
    country_code = 'DE'
GROUP BY
    country_code, store_type
ORDER BY
    total_sales;


-- Task 9. How quickly is the company making sales?
WITH all_orders_by_time AS (
SELECT
    year,
    TO_TIMESTAMP(year || '-' || month || '-' || day || ' ' || timestamp, 'YYYY-MM-DD HH24:MI:SS') AS timestamp
FROM
    orders_table AS ot
INNER JOIN
    dim_date_times AS ddt ON ot.date_uuid = ddt.date_uuid
ORDER BY
    timestamp
), lead_table AS (
SELECT
    year,
    timestamp,
    LEAD(timestamp) OVER (ORDER BY timestamp) AS next
FROM
    all_orders_by_time
)
SELECT
    year,
    AVG(next - timestamp) AS actual_time_taken
FROM
    lead_table
GROUP BY
    year
ORDER BY
    actual_time_taken DESC
LIMIT 5;