{{ config(
    materialized='table',
    schema='silver'
) }}

SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp::TIMESTAMP AS order_purchase_timestamp,
    order_approved_at::TIMESTAMP AS order_approved_at,
    order_delivered_carrier_date::TIMESTAMP AS order_delivered_carrier_date,
    order_delivered_customer_date::TIMESTAMP AS order_delivered_customer_date,
    order_estimated_delivery_date::TIMESTAMP AS order_estimated_delivery_date,
    loaded_at,
    CURRENT_TIMESTAMP AS transformed_at
FROM {{ source('bronze', 'orders') }}
WHERE order_id IS NOT NULL
  AND order_status IS NOT NULL