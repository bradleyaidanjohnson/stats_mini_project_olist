# stats_mini_project_olist

# Cleaning Guide

## Cleaning order payments

SELECT MAX(payment_installments) as max_installments
, AVG(payment_installments) AS avg_installments
, MAX(payment_sequential) AS max_sequential
, AVG(payment_sequential) AS avg_sequential
, MAX(payment_value) AS max_value
, AVG(payment_value) AS avg_value
FROM `integral-cell-418310.Olist.olist_order_payments_dataset`;

WITH test AS(
SELECT order_id
, COUNT(payment_sequential) AS count_transactions
, MAX(payment_sequential) AS max_sequential
FROM `integral-cell-418310.Olist.olist_order_payments_dataset`
GROUP BY order_id
)
SELECT \*
FROM test
WHERE test.count_transactions <> test.max_sequential;

SELECT order_id
, MAX(payment_installments) AS max_installments
, COUNT(order_id) AS total_transactions
FROM `integral-cell-418310.Olist.olist_order_payments_dataset`
GROUP BY order_id
ORDER BY max_installments DESC;

### Irreconcilable Transactions

WITH max_seq AS(
SELECT order_id
, MAX(payment_sequential) AS max_sequential
, COUNT(\*) AS count_transactions
FROM `Olist.olist_order_payments_dataset`
GROUP BY order_id
HAVING max_sequential > count_transactions
)
SELECT orders.order_id
, payment_type
, payment_sequential
, payment_installments
, payment_value
FROM `Olist.olist_order_payments_dataset` orders
JOIN max_seq
ON max_seq.order_id = orders.order_id;

WITH total*transactions AS(
SELECT COUNT(*)
FROM `Olist.olist_order_payments_dataset`
)
SELECT payment*type
, COUNT(*) AS total*transactions
, ROUND(SAFE_DIVIDE(COUNT(*), (SELECT _ FROM total_transactions)), 3) AS transactions_perc
FROM `Olist.olist_order_payments_dataset`
GROUP BY payment_type
ORDER BY COUNT(_) DESC;
/\_
credit_card
boleto
voucher
debit_card
not_defined

there are no instances of sequential higher than installments other than when installments is 1
\*/

## Customers basic facts

### Basic Facts

### Total Customers

SELECT COUNT(customer_id) AS total_customers
FROM `integral-cell-418310.Olist.olist_customers_dataset`;

### Most Popular ZIPs

SELECT customer_zip_code_prefix
, COUNT(\*) AS total_cutomers
FROM `integral-cell-418310.Olist.olist_customers_dataset`
GROUP BY customer_zip_code_prefix
ORDER BY total_cutomers DESC
LIMIT 20;

### Most Popular Cities

SELECT customer*city
, COUNT(*) AS total*cutomers
, ROUND(SAFE_DIVIDE(COUNT(*), (SELECT COUNT(customer_id) FROM `integral-cell-418310.Olist.olist_customers_dataset`)), 3) AS percent_of_customers
FROM `integral-cell-418310.Olist.olist_customers_dataset`
GROUP BY customer_city
ORDER BY total_cutomers DESC
LIMIT 20;

### Most Popular States

SELECT customer*state
, COUNT(*) AS total*cutomers
, ROUND(SAFE_DIVIDE(COUNT(*), (SELECT COUNT(customer_id) FROM `integral-cell-418310.Olist.olist_customers_dataset`)), 3) AS percent_of_customers
FROM `integral-cell-418310.Olist.olist_customers_dataset`
GROUP BY customer_state
ORDER BY total_cutomers DESC;

## Olist EDA Bottlenecks

### Percentage bottlenecks for late orders

WITH total_late AS(
SELECT COUNT(\*)
FROM `Olist.olist_orders_dataset_finished_view`
WHERE late = 1
)

SELECT bottleneck
, COUNT(_)
, ROUND(SAFE_DIVIDE(COUNT(_), (SELECT \* FROM total_late)), 3) AS percent_of_total
FROM `Olist.olist_orders_dataset_finished_view`
WHERE late = 1
GROUP BY bottleneck;

### Percentage bottlenecks for on time orders

WITH total_late AS(
SELECT COUNT(\*)
FROM `Olist.olist_orders_dataset_finished_view`
WHERE late = 0
)

SELECT bottleneck
, COUNT(_)
, ROUND(SAFE_DIVIDE(COUNT(_), (SELECT \* FROM total_late)), 3) AS percent_of_total
FROM `Olist.olist_orders_dataset_finished_view`
WHERE late = 0
GROUP BY bottleneck;

### Percentage bottlenecks over months for late orders

WITH total_late AS(
SELECT COUNT(\*)
FROM `Olist.olist_orders_dataset_finished_view`
WHERE late = 1
)

SELECT EXTRACT(MONTH FROM date*purchased) AS month
, EXTRACT(YEAR FROM date_purchased) AS year
, bottleneck
, COUNT(*) AS occurences
, ROUND(SAFE*DIVIDE(COUNT(*), (SELECT \* FROM total_late)), 3) AS percent_of_total
FROM `Olist.olist_orders_dataset_finished_view`
WHERE late = 1
GROUP BY year, month, bottleneck
ORDER BY year, month, bottleneck;

### Percentage bottlenecks over months for on time orders

WITH total_late AS(
SELECT COUNT(\*)
FROM `Olist.olist_orders_dataset_finished_view`
WHERE late = 0
)

SELECT EXTRACT(MONTH FROM date*purchased) AS month
, EXTRACT(YEAR FROM date_purchased) AS year
, bottleneck
, COUNT(*) AS occurences
, ROUND(SAFE*DIVIDE(COUNT(*), (SELECT \* FROM total_late)), 3) AS percent_of_total
FROM `Olist.olist_orders_dataset_finished_view`
WHERE late = 0
GROUP BY year, month, bottleneck
ORDER BY year, month, bottleneck;

Olist EDA Late Order Percent Over Time

SELECT ROUND(SAFE_DIVIDE(SUM(late), COUNT(late)), 3) AS late_percent
FROM `Olist.olist_orders_dataset_finished_view`;

SELECT EXTRACT(MONTH FROM date_purchased) AS month
, EXTRACT(YEAR FROM date_purchased) AS year
, ROUND(SAFE_DIVIDE(SUM(late), COUNT(late)), 3) AS late_percent
FROM `Olist.olist_orders_dataset_finished_view`
GROUP BY year, month
ORDER BY year, month

## Olist EDA Order Item Trends Over Time

WITH max_prices AS(
SELECT year
, month
, order_id
, MAX(price) AS max_price
FROM `Olist.olist_order_items_dataset_finished`
GROUP BY year, month, order_id
)
, avg_max_prices AS(
SELECT year
, month
, AVG(max_price) AS avg_max_price
FROM max_prices
GROUP BY year, month
ORDER BY year, month
)
, main_data AS (
SELECT year
, month
, ROUND(AVG(total_products_per_order), 2) AS avg_products_per_order
, ROUND(AVG(total_order_value), 2) AS avg_order_value
, MAX(price) AS max_order_item -- change this to be average of the maxes for the period
, ROUND(AVG(total_order_freight), 2) AS avg_freight_value
, COUNT(DISTINCT IF(total_products_per_order = total_specific_product_per_order, order_id, NULL)) AS single_product_orders
, COUNT(DISTINCT order_id) AS total_orders
, ROUND(SAFE_DIVIDE(COUNT(DISTINCT IF(total_products_per_order = total_specific_product_per_order, order_id, NULL)), COUNT(DISTINCT order_id)), 3) single_product_orders_perc
FROM `Olist.olist_order_items_dataset_finished` order_items
GROUP BY year, month
)
SELECT main_data.year
, main_data.month
, avg_products_per_order
, avg_order_value
, max_order_item
, avg_freight_value
, single_product_orders
, total_orders
, single_product_orders_perc

, ROUND(avg_max_price, 2) AS avg_max_price
FROM main_data
JOIN avg_max_prices
ON main_data.year = avg_max_prices.year
AND main_data.month = avg_max_prices.month
ORDER BY year, month

## Olist Key Questions

/\*
I - Getting to Know the Context - 30min/1H

Understanding the Business Context

Identifying key business questions related to Olist.

Customer-Centric Questions:
\*/

### Who are Olist’s high-value customers?

SELECT customer_unique_id
, SUM(total_order_value) total_spend
FROM `Olist.olist_orders_dataset_finished_view` orders
JOIN `Olist.olist_order_payments_orderids` order_payments
ON orders.order_id = order_payments.order_id
JOIN `Olist.olist_customers_dataset` customers
ON orders.customer_id = customers.customer_id
GROUP BY customer_unique_id
ORDER BY total_spend DESC;

### What is the average lifetime value of a customer on Olist?

WITH customer_spends AS(
SELECT customer_unique_id
, SUM(total_order_value) total_spend
FROM `Olist.olist_orders_dataset_finished_view` orders
JOIN `Olist.olist_order_payments_orderids` order_payments
ON orders.order_id = order_payments.order_id
JOIN `Olist.olist_customers_dataset` customers
ON orders.customer_id = customers.customer_id
GROUP BY customer_unique_id
)
SELECT ROUND(AVG(total_spend), 2) AS customer_lifetime_value
FROM customer_spends;

### How frequently do customers make repeat purchases?

WITH customer*spends AS(
SELECT customer_unique_id
, COUNT(*) nb*orders
FROM `Olist.olist_orders_dataset_finished_view` orders
JOIN `Olist.olist_order_payments_orderids` order_payments
ON orders.order_id = order_payments.order_id
JOIN `Olist.olist_customers_dataset` customers
ON orders.customer_id = customers.customer_id
GROUP BY customer_unique_id
)
SELECT SUM(CASE
WHEN customer_spends.nb_orders > 1 THEN 1
ELSE 0
END) AS total_repeat_customers
, COUNT(*) AS total_customers
, ROUND(SAFE_DIVIDE(
SUM(CASE
WHEN customer_spends.nb_orders > 1 THEN 1
ELSE 0
END), COUNT(\*)), 3
) AS repeat_customer_perc
FROM customer_spends;

### Product and Sales Questions:

### Which products are the best-sellers on Olist?

### Turnover

SELECT products.product*id
, product_category_name_en
, ROUND(SUM(price), 2) AS total_revenue
, COUNT(*) AS total_qty_sold
FROM `Olist.olist_products_dataset_view` products
JOIN `Olist.olist_order_items_dataset_finished` orders_items
ON products.product_id = orders_items.product_id
GROUP BY products.product_id, product_category_name_en
ORDER BY total_revenue DESC, total_qty_sold DESC;

### Quantity

SELECT products.product*id
, product_category_name_en
, ROUND(SUM(price), 2) AS total_revenue
, COUNT(*) AS total_qty_sold
FROM `Olist.olist_products_dataset_view` products
JOIN `Olist.olist_order_items_dataset_finished` orders_items
ON products.product_id = orders_items.product_id
GROUP BY products.product_id, product_category_name_en
ORDER BY total_qty_sold DESC, total_revenue DESC;

### What is the overall revenue trend for Olist over time?

SELECT year
, month
, ROUND(SUM(price), 2) AS total_revenue
FROM `Olist.olist_order_items_dataset_finished`
GROUP BY year, month
ORDER BY year, month;

### Are there specific product categories that contribute significantly to revenue?

SELECT product_category_name_en
, ROUND(SUM(price), 2) AS total_revenue
, COUNT(\*) AS total_qty_sold
FROM `Olist.olist_products_dataset_view` products
JOIN `Olist.olist_order_items_dataset_finished` orders_items
ON products.product_id = orders_items.product_id
GROUP BY product_category_name_en
ORDER BY total_revenue DESC, total_qty_sold DESC;

### Operational Efficiency Questions:

### What is the average delivery time for orders on Olist?

SELECT ROUND(AVG(carr_2_del), 1) AS avg_delivery_time
FROM `Olist.olist_orders_dataset_finished_view`;

### Are there specific sellers or regions experiencing delays?

### by delivery state

SELECT customer*state
, SUM(late) AS total_late_orders
, COUNT(*) AS total*orders
, ROUND(SAFE_DIVIDE(SUM(late), COUNT(*)), 3) AS late*perc
FROM `Olist.olist_orders_dataset_finished_view` orders
JOIN `Olist.olist_customers_dataset` customers
ON orders.customer_id = customers.customer_id
GROUP BY customer_state
ORDER BY SAFE_DIVIDE(SUM(late), COUNT(*)) DESC;

### by seller (% late)

SELECT seller*id
, SUM(late) AS total_late_orders
, COUNT(*) AS total*orders
, ROUND(SAFE_DIVIDE(SUM(late), COUNT(*)), 3) AS late*perc
FROM `Olist.olist_orders_dataset_finished_view` orders
JOIN `Olist.olist_order_items_dataset_finished` order_items
ON orders.order_id = order_items.order_id
GROUP BY seller_id
ORDER BY SAFE_DIVIDE(SUM(late), COUNT(*)) DESC;

### by seller (total late)

SELECT seller*id
, SUM(late) AS total_late_orders
, COUNT(*) AS total*orders
, ROUND(SAFE_DIVIDE(SUM(late), COUNT(*)), 3) AS late_perc
FROM `Olist.olist_orders_dataset_finished_view` orders
JOIN `Olist.olist_order_items_dataset_finished` order_items
ON orders.order_id = order_items.order_id
GROUP BY seller_id
ORDER BY total_late_orders DESC;

### How does order status impact customer satisfaction?

SELECT order_status
, ROUND(AVG(review_score), 2) AS avg_review_score
, ROUND(SAFE_DIVIDE(SUM(prom_detra), COUNT(\*)), 1) AS prom_detra
FROM `Olist.olist_order_reviews_dataset_view` reviews
JOIN `Olist.olist_orders_dataset_finished_view` orders
ON reviews.order_id = orders.order_id
GROUP BY order_status
ORDER BY avg_review_score DESC;

### Marketplace Dynamics Questions:

### How many sellers are actively participating on Olist?

SELECT COUNT(DISTINCT seller_id) AS participating_sellers
FROM `Olist.olist_sellers_dataset`;

### What is the distribution of products across different sellers?

SELECT seller_id
, COUNT(DISTINCT product_id) AS different_products_sold
FROM `Olist.olist_order_items_dataset_finished`
GROUP BY seller_id
ORDER BY different_products_sold DESC;

WITH product_dist AS(
SELECT COUNT(DISTINCT product_id) AS different_products_sold
FROM `Olist.olist_order_items_dataset_finished`
GROUP BY seller_id
ORDER BY different_products_sold DESC
)
SELECT ROUND(AVG(different_products_sold), 2) AS avg_products_per_seller
FROM product_dist;

### Are there seasonal patterns in seller and product activity?

SELECT year
, month
, COUNT(DISTINCT seller_id) total_sellers
, COUNT(DISTINCT product_id) total_products
, ROUND(SAFE_DIVIDE(COUNT(DISTINCT product_id), COUNT(DISTINCT seller_id)), 3) AS products_per_seller
FROM `Olist.olist_order_items_dataset_finished`
GROUP BY year, month
ORDER BY year, month;

### Customer Review and Satisfaction Questions:

### What is the overall customer satisfaction score based on reviews?

SELECT ROUND(AVG(review*score), 2) AS avg_review_score
, ROUND(SAFE_DIVIDE(SUM(prom_detra), COUNT(*)), 1) AS prom_detra
FROM `Olist.olist_order_reviews_dataset_view` reviews;

### Are there correlations between product categories and review scores?

SELECT product*category_name_en
, ROUND(AVG(review_score), 2) AS avg_review_score
, ROUND(SAFE_DIVIDE(SUM(prom_detra), COUNT(*)), 1) AS prom_detra
FROM `Olist.olist_order_reviews_dataset_view` reviews
JOIN `Olist.olist_order_items_dataset_finished` order_items
ON order_items.order_id = reviews. order_id
JOIN `Olist.olist_products_dataset_view` products
ON order_items.product_id = products.product_id
GROUP BY product_category_name_en
ORDER BY avg_review_score DESC;

### Geographic and Demographic Questions:

### Which regions or states contribute the most to Olist’s revenue?

SELECT customer_state
, ROUND(SUM(price), 2) AS total_revenue
FROM `Olist.olist_order_items_dataset_finished` order_items
JOIN `Olist.olist_orders_dataset_finished_view` orders
ON order_items.order_id = orders.order_id
JOIN `Olist.olist_customers_dataset` customers
ON orders.customer_id = customers.customer_id
GROUP BY customer_state
ORDER BY total_revenue DESC;

### Are there differences in customer behavior based on geographic location?

### What is the distribution of sellers and customers across different cities?

WITH city_customers AS(
SELECT customer_city
, COUNT(DISTINCT customer_unique_id) AS total_customers
FROM `Olist.olist_customers_dataset`
GROUP BY customer_city
)
, city_sellers AS(
SELECT seller_city
, COUNT(DISTINCT seller_id) AS total_sellers
FROM `Olist.olist_sellers_dataset`
GROUP BY seller_city
)
SELECT customer_city AS city
, city_customers.total_customers
, city_sellers.total_sellers
, ROUND(SAFE_DIVIDE(city_customers.total_customers, city_sellers.total_sellers), 1) AS customers_per_seller
FROM city_customers
JOIN city_sellers
ON city_customers.customer_city = city_sellers.seller_city
ORDER BY total_customers DESC;

### Revenue by Product Category

SELECT product_category_name_en
, ROUND(SUM(price), 2) AS total_revenue
FROM `Olist.olist_order_items_dataset_finished` order_items
JOIN `Olist.olist_products_dataset_view` products
ON order_items.product_id = products.product_id
GROUP BY product_category_name_en
ORDER BY total_revenue DESC;

### Number of Orders Over Time

SELECT year
, month
, COUNT(DISTINCT order_id) AS total_orders
FROM `Olist.olist_order_items_dataset_finished` order_items
GROUP BY year, month
ORDER BY year, month;

### Average Time between Orders

### TOP CUSTOMERS

SELECT customer_unique_id
, ROUND(SUM(price), 2) total_revenue
, COUNT(DISTINCT orders.order_id) AS total_orders
, COUNT(\*) AS total_products
FROM `Olist.olist_order_items_dataset_finished` order_items
JOIN `Olist.olist_orders_dataset_finished_view` orders
ON order_items.order_id = orders.order_id
JOIN `Olist.olist_customers_dataset` customers
ON orders.customer_id = customers.customer_id
GROUP BY customer_unique_id
ORDER BY total_revenue DESC;

Olist Orders Basic Facts

### Basic Facts

SELECT COUNT(DISTINCT order_id) AS total_orders
, COUNT(DISTINCT customer_id) AS total_customers
, SUM(late) total_late
, Olist.date_diff_percent(SUM(late), COUNT(order_id)) AS late_percent
, SUM(canceled) AS total_canceled
, Olist.date_diff_percent(SUM(canceled), COUNT(order_id)) AS canceled_percent
, SUM(unavailable) AS total_unavailable
, Olist.date_diff_percent(SUM(unavailable), COUNT(order_id)) AS unavailable_percent
FROM `Olist.olist_orders_dataset_finished_view`;

### Basic Facts Over Time

SELECT EXTRACT(MONTH FROM date_purchased) AS month
, EXTRACT(YEAR FROM date_purchased) AS year
, COUNT(DISTINCT order_id) AS total_orders
, COUNT(DISTINCT customer_id) AS total_customers
, SUM(late) total_late
, Olist.date_diff_percent(SUM(late), COUNT(order_id)) AS late_percent
, SUM(canceled) AS total_canceled
, Olist.date_diff_percent(SUM(canceled), COUNT(order_id)) AS canceled_percent
, SUM(unavailable) AS total_unavailable
, Olist.date_diff_percent(SUM(unavailable), COUNT(order_id)) AS unavailable_percent
FROM `Olist.olist_orders_dataset_finished_view`
GROUP BY Year, Month
ORDER BY Year, Month;
/\*

\*/

## Olist Payments Clean

UPDATE `Olist.olist_order_payments_dataset_clean`
SET payment*sequential = payment_sequential - 1
--SELECT order_id, payment_sequential
--FROM `Olist.olist_order_payments_dataset`
WHERE order_id IN (
SELECT order_id
FROM (
SELECT order_id
, MAX(payment_sequential) AS max_sequential
, COUNT(*) AS count*transactions
FROM `Olist.olist_order_payments_dataset_clean`
GROUP BY order_id
) AS max_seq
WHERE max_sequential > count_transactions
)
/*
SELECT _
FROM `Olist.olist_order_payments_dataset`
_/

## Olist Products Basic Facts

SELECT COUNT(\*) AS num_products
, ROUND(AVG(product_volume_cm3), 1) AS avg_volume
, ROUND(AVG(product_weight_g), 1) AS avg_weight
, COUNT(DISTINCT product_category_name) AS num_categories
FROM `Olist.olist_products_dataset_view`;

SELECT product_category_name_en
, COUNT(\*) AS total_products
, ROUND(AVG(product_volume_cm3), 1) AS avg_volume
, ROUND(AVG(product_weight_g), 1) AS avg_weight
FROM `Olist.olist_products_dataset_view`
GROUP BY product_category_name_en
ORDER BY product_category_name_en;

/\*products per category
avg weight per category
avg volume per category

\*/

## Olist Reviews Basic Facts

### Basic Facts

### Overall Sentiment

SELECT SUM(prom_detra) AS overall_sentiment
, ROUND(SAFE_DIVIDE(SUM(prom_detra), COUNT(\*)), 3) AS promoter_perc
, COUNT(order_id) AS orders_reviewed
FROM Olist.olist_order_reviews_dataset_view;

### Overall Response Times

SELECT MAX(response_time) AS max_response_time
, AVG(response_time) AS avg_response_time
FROM Olist.olist_order_reviews_dataset_view;

### Response Times Over Time

SELECT creation_year
, creation_month
, MAX(response_time) AS max_response_time
, AVG(response_time) AS avg_response_time
, COUNT(order_id) AS orders_reviewed
FROM Olist.olist_order_reviews_dataset_view
GROUP BY creation_year, creation_month
ORDER BY creation_year, creation_month;

### Reviews Per Response Time Category

SELECT response_time_cat
, COUNT(\*) AS reviews
FROM Olist.olist_order_reviews_dataset_view
GROUP BY response_time_cat
ORDER BY response_time_cat;

### Reviews Per Score

SELECT review_score
, COUNT(\*) AS reviews
, COUNT(order_id) AS orders_reviewed
FROM Olist.olist_order_reviews_dataset_view
GROUP BY review_score
ORDER BY review_score;

### Review Senteiment Over Time

SELECT creation_year
, creation_month
, SUM(prom_detra) AS overall_sentiment
, ROUND(SAFE_DIVIDE(SUM(prom_detra), COUNT(\*)), 3) AS promoter_perc
, COUNT(order_id) AS orders_reviewed
FROM Olist.olist_order_reviews_dataset_view
GROUP BY creation_year, creation_month
ORDER BY creation_year, creation_month;

### Review Sentiment By Response Time Category

SELECT response_time_cat
, SUM(prom_detra) AS overall_sentiment
, ROUND(SAFE_DIVIDE(SUM(prom_detra), COUNT(\*)), 3) AS promoter_perc
, COUNT(order_id) AS orders_reviewed
FROM Olist.olist_order_reviews_dataset_view
GROUP BY response_time_cat
ORDER BY response_time_cat;

## Olist Sellers Basic Facts

### Basic Facts

### Total Sellers

SELECT COUNT(seller_id) AS total_sellers
FROM `integral-cell-418310.Olist.olist_sellers_dataset`;

### Most Popular ZIPs

SELECT seller_zip_code_prefix
, COUNT(\*) AS total_sellers
FROM `integral-cell-418310.Olist.olist_sellers_dataset`
GROUP BY seller_zip_code_prefix
ORDER BY total_sellers DESC
LIMIT 20;

### Most Popular Cities

SELECT seller*city
, COUNT(*) AS total*sellers
, ROUND(SAFE_DIVIDE(COUNT(*), (SELECT COUNT(seller_id) FROM `integral-cell-418310.Olist.olist_sellers_dataset`)), 3) AS percent_of_sellers
FROM `integral-cell-418310.Olist.olist_sellers_dataset`
GROUP BY seller_city
ORDER BY total_sellers DESC
LIMIT 20;

### Most Popular States

SELECT seller*state
, COUNT(*) AS total*sellers
, ROUND(SAFE_DIVIDE(COUNT(*), (SELECT COUNT(seller_id) FROM `integral-cell-418310.Olist.olist_sellers_dataset`)), 3) AS percent_of_sellers
FROM `integral-cell-418310.Olist.olist_sellers_dataset`
GROUP BY seller_state
ORDER BY total_sellers DESC;

## Olist Shipping Basic Facts

### Basic Facts

SELECT COUNT(DISTINCT order_id) AS number_of_orders
, COUNT(DISTINCT product_id) AS number_of_distinct_products
, COUNT(product_id) AS number_of_products
, COUNT(DISTINCT seller_id) AS number_of_sellers
, ROUND(SUM(freight_value), 2) AS total_freight
, ROUND(AVG(price), 2) AS avg_price
FROM Olist.olist_order_items_dataset_formatted;

### Basic Facts Over Time

SELECT EXTRACT(MONTH FROM shipping_limit_date) AS month
, EXTRACT(YEAR FROM shipping_limit_date) AS year
, COUNT(DISTINCT order_id) AS number_of_orders
, COUNT(DISTINCT product_id) AS number_of_distinct_products
, COUNT(product_id) AS number_of_products
, ROUND(SAFE_DIVIDE(COUNT(product_id), COUNT(DISTINCT order_id)),1) AS products_per_order
, COUNT(DISTINCT seller_id) AS number_of_sellers
, ROUND(SUM(freight_value), 2) AS total_freight
, ROUND(AVG(price), 2) AS avg_price
FROM Olist.olist_order_items_dataset_formatted
GROUP BY year, month
ORDER BY year, month

## Olist Views

### olist_order_items_dataset_finished

SELECT shipping*limit_date
, EXTRACT(YEAR FROM shipping_limit_date) AS year
, EXTRACT(MONTH FROM shipping_limit_date) AS month
, order_id
, seller_id
, product_id
, order_item_id
, COUNT(*) OVER(PARTITION BY order*id, product_id) AS total_specific_product_per_order
, COUNT(*) OVER(PARTITION BY order*id) AS total_products_per_order
, price
, ROUND(SUM(price) OVER(PARTITION BY order_id, product_id), 2) AS total_products_value
, ROUND(SUM(price) OVER(PARTITION BY order_id), 2) AS total_order_value
, ROUND(SAFE_DIVIDE(price, SUM(price) OVER(PARTITION BY order_id)), 3) AS ind_product_price_perc
, ROUND(SAFE_DIVIDE(COUNT(*) OVER(PARTITION BY order*id, product_id) * price, SUM(price) OVER(PARTITION BY order*id)), 3) AS products_price_perc
, MAX(price) OVER(PARTITION BY order_id) AS order_max_price
, freight_value
, ROUND(SUM(freight_value) OVER(PARTITION BY order_id), 2) AS total_order_freight
, ROUND(SAFE_DIVIDE(freight_value, SUM(freight_value) OVER(PARTITION BY order_id)), 3) individual_product_freight_perc
, ROUND(SAFE_DIVIDE(freight_value * COUNT(\_) OVER(PARTITION BY order_id, product_id), SUM(freight_value) OVER(PARTITION BY order_id)), 3) products_freight_perc
, ROUND(SAFE_DIVIDE(price, freight_value), 3) ind_product_value2freight
, ROUND(SAFE_DIVIDE(SUM(price) OVER(PARTITION BY order_id, product_id), SUM(freight_value) OVER(PARTITION BY order_id)), 3) products_value2freight
, ROUND(SAFE_DIVIDE(SUM(price) OVER(PARTITION BY order_id), SUM(freight_value) OVER(PARTITION BY order_id)), 3) order_value2freight
FROM `Olist.olist_order_items_dataset_formatted`
ORDER BY shipping_limit_date, order_id, product_id, order_item_id

### olist_order_payments_dataset_view

WITH voucherless*ids AS(
WITH distinct_ids AS(
SELECT DISTINCT order_id
FROM `Olist.olist_order_payments_dataset_clean`
)
SELECT DISTINCT distinct_ids.order_id
FROM `Olist.olist_order_payments_dataset_clean` c1
RIGHT JOIN distinct_ids
ON c1.order_id = distinct_ids.order_id
AND c1.payment_type = 'voucher'
WHERE c1.order_id IS NULL
)
SELECT order_id
, payment_sequential
, MAX(payment_sequential) OVER(PARTITION BY order_id) AS total_order_payments
, SUM(CASE
WHEN payment_type = 'voucher' THEN 0
ELSE 1
END) OVER(PARTITION BY order_id) AS total_order_payments_less_voucher
, IF(MAX(payment_sequential) OVER(PARTITION BY order_id) >
SUM(
CASE
WHEN payment_type = 'voucher' THEN 1
ELSE 0
END) OVER(PARTITION BY order_id) + 1, 1, 0) AS installment_payment
, payment_type
, payment_value
, ROUND(SUM(payment_value) OVER(PARTITION BY order_id), 2) AS total_order_value
, ROUND(AVG(payment_value) OVER(PARTITION BY order_id), 2) AS avg_order_payment
, IF(COUNT(DISTINCT payment_type) OVER (PARTITION BY order_id) > 1, 1, 0) AS multi_type_payment
, IF(order_id IN (SELECT * FROM voucherless*ids), 0, 1) AS voucher_used
, SUM(CASE
WHEN payment_type = 'voucher' THEN 1
ELSE 0
END) OVER(PARTITION BY order_id) AS vouchers_used
, IF(COUNT(DISTINCT payment_type) OVER (PARTITION BY order_id) > 2
AND order_id IN (SELECT * FROM voucherless*ids), 1,
IF(COUNT(DISTINCT payment_type) OVER (PARTITION BY order_id) > 1
AND order_id IN (SELECT * FROM voucherless*ids), 1, 0)) AS true_multi_payment
, IF(order_id NOT IN (SELECT * FROM voucherless_ids)
AND COUNT(DISTINCT payment_type) OVER (PARTITION BY order_id) = 1, 1, 0) AS voucher_only
FROM `Olist.olist_order_payments_dataset_clean`
ORDER BY order_id, payment_sequential

### olist_order_payments_orderids

SELECT order_id
, IF(SUM(installment_payment) > 1, 1, 0) AS installment_payment
, MAX(total_order_payments_less_voucher) AS total_order_payments_less_voucher
, MAX(total_order_value) AS total_order_value
, MAX(avg_order_payment) AS avg_order_payment
, IF(SUM(multi_type_payment) > 0, 1, 0) AS multi_type_payment
, IF(SUM(voucher_used) > 0, 1, 0) AS voucher_used
, MAX(vouchers_used) AS vouchers_used
FROM `Olist.olist_order_payments_dataset_view`
GROUP BY order_id

### olist_order_reviews_dataset_view

WITH cleaned AS(
SELECT review_id
, order_id
, review_score
, IF(review_score > 3, 1,(IF(review_score < 3, -1, 0))) AS prom_detra
, CAST(review_creation_date AS DATE) AS review_creation_date
, CAST(review_answer_timestamp AS DATE) AS review_answer_date
, DATE_DIFF(CAST(review_answer_timestamp AS DATE), CAST(review_creation_date AS DATE), DAY) AS response_time
FROM `Olist.reviews_test`
)
SELECT EXTRACT(YEAR FROM review_creation_date) AS creation_year
, EXTRACT(Month FROM review_creation_date) AS creation_month
, \*
, CASE
WHEN response_time = 0 THEN '0_days'
WHEN response_time = 1 THEN '1_day'
WHEN response_time = 2 THEN '2_days'
WHEN response_time = 3 THEN '3_days'
WHEN response_time = 3 THEN '3_7_days'
ELSE 'week_plus'
END AS response_time_cat
FROM cleaned
ORDER BY cleaned.review_creation_date

### olist_orders_dataset_finished_view

WITH durations*added AS(
SELECT *
, DATE*DIFF(date_approved, date_purchased, DAY) AS purch_2_appr
, DATE_DIFF(date_carrier, date_purchased, DAY) AS purch_2_carr
, DATE_DIFF(date_delivered, date_purchased, DAY) AS purch_2_del
, DATE_DIFF(date_estimated, date_purchased, DAY) AS purch_2_est
, DATE_DIFF(date_carrier, date_approved, DAY) AS appr_2_carr
, DATE_DIFF(date_delivered, date_approved, DAY) AS appr_2_del
, DATE_DIFF(date_estimated, date_approved, DAY) AS appr_2_est
, DATE_DIFF(date_delivered, date_carrier, DAY) AS carr_2_del
, DATE_DIFF(date_estimated, date_carrier, DAY) AS carr_2_est
, DATE_DIFF(date_estimated, date_delivered, DAY) AS del_2_est
FROM `Olist.olist_orders_dataset_formatted`
)
, percs_added AS(
SELECT *
, IF (durations*added.date_delivered > durations_added.date_estimated, 1,IF(durations_added.date_delivered IS NULL, NULL, 0)) AS late
, Olist.date_diff_percent(durations_added.purch_2_appr, durations_added.purch_2_del) AS approval_perc
, Olist.date_diff_percent(durations_added.appr_2_carr, durations_added.purch_2_del) AS to_carrier_perc
, Olist.date_diff_percent(durations_added.carr_2_del, durations_added.purch_2_del) AS delivery_perc
FROM durations_added
)
SELECT *
, CASE
WHEN order*status = 'canceled' THEN 1
ELSE 0
END AS canceled
, CASE
WHEN order_status = 'unavailable' THEN 1
ELSE 0
END AS unavailable
, CASE
WHEN percs_added.delivery_perc > percs_added.to_carrier_perc
AND percs_added.delivery_perc > percs_added.approval_perc THEN 'delivery'
WHEN percs_added.to_carrier_perc > percs_added.delivery_perc
AND percs_added.to_carrier_perc > percs_added.approval_perc THEN 'to carrier'
ELSE 'approval'
END AS bottleneck
, EXTRACT (YEAR FROM date_purchased) AS year
, EXTRACT (MONTH FROM date_purchased) AS month
FROM percs_added
/*
CREATE OR REPLACE FUNCTION Olist.date*diff_percent(numerator INT64, denominator INT64)
AS(
ROUND(SAFE_DIVIDE(numerator, denominator), 3)
)
*/
/\_

across the funnel
bottlenecks

recommendations on bottlenecks
group by dates/ months
find anomalies
liase with client

\*/

### olist_products_dataset_view

SELECT product_id
, product_category_name
, string_field_1 AS product_category_name_en
, product_weight_g
, product_length_cm _ product_height_cm _ product_width_cm AS product_volume_cm3
FROM `Olist.test_products` database
LEFT JOIN `Olist.test_translations` translations
ON database.product_category_name = translations.string_field_0
