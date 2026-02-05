{{ config(
    materialized='table',
    schema='silver'
) }}

SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    price::NUMERIC(10,2) AS price,
    freight_value::NUMERIC(10,2) AS freight_value,
    loaded_at,
    CURRENT_TIMESTAMP AS transformed_at
FROM {{ source('bronze', 'order_items') }}
WHERE order_id IS NOT NULL
  AND product_id IS NOT NULL
  AND price >= 0