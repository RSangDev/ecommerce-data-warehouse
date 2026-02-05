

SELECT
    customer_id,
    customer_unique_id,
    UPPER(TRIM(customer_zip_code_prefix::TEXT)) AS customer_zip_code,
    UPPER(TRIM(customer_city)) AS customer_city,
    UPPER(TRIM(customer_state)) AS customer_state,
    loaded_at,
    CURRENT_TIMESTAMP AS transformed_at
FROM "warehouse"."bronze"."customers"
WHERE customer_id IS NOT NULL