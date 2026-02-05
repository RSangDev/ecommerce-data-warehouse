{{ config(
    materialized='table',
    schema='gold'
) }}

WITH orders AS (
    SELECT * FROM {{ ref('silver_orders') }}
),

order_items AS (
    SELECT * FROM {{ ref('silver_order_items') }}
),

customers AS (
    SELECT * FROM {{ ref('silver_customers') }}
),

sales_data AS (
    SELECT
        c.customer_state,
        DATE_TRUNC('month', o.order_purchase_timestamp) AS order_month,
        COUNT(DISTINCT o.order_id) AS total_orders,
        COUNT(DISTINCT o.customer_id) AS unique_customers,
        SUM(oi.price) AS total_revenue,
        SUM(oi.freight_value) AS total_freight,
        AVG(oi.price) AS avg_order_value,
        MIN(o.order_purchase_timestamp) AS first_order,
        MAX(o.order_purchase_timestamp) AS last_order
    FROM orders o
    INNER JOIN customers c ON o.customer_id = c.customer_id
    INNER JOIN order_items oi ON o.order_id = oi.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY 1, 2
)

SELECT
    *,
    CURRENT_TIMESTAMP AS calculated_at
FROM sales_data
ORDER BY order_month DESC, total_revenue DESC