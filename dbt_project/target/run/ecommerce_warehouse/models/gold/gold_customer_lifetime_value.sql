
  
    

  create  table "warehouse"."public_gold"."gold_customer_lifetime_value__dbt_tmp"
  
  
    as
  
  (
    

WITH customer_orders AS (
    SELECT
        c.customer_id,
        c.customer_state,
        c.customer_city,
        COUNT(DISTINCT o.order_id) AS total_orders,
        MIN(o.order_purchase_timestamp) AS first_order_date,
        MAX(o.order_purchase_timestamp) AS last_order_date,
        SUM(oi.price + oi.freight_value) AS lifetime_value,
        AVG(oi.price + oi.freight_value) AS avg_order_value
    FROM "warehouse"."public_silver"."silver_customers" c
    INNER JOIN "warehouse"."public_silver"."silver_orders" o ON c.customer_id = o.customer_id
    INNER JOIN "warehouse"."public_silver"."silver_order_items" oi ON o.order_id = oi.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY 1, 2, 3
)

SELECT
    *,
    EXTRACT(DAY FROM (last_order_date - first_order_date)) AS customer_lifetime_days,
    CURRENT_TIMESTAMP AS calculated_at
FROM customer_orders
ORDER BY lifetime_value DESC
  );
  