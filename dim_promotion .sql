-- dim_promotion
{{
  config(
    materialized='table'
  )
}}

WITH t_data AS (
  SELECT DISTINCT 
    COALESCE(`promotion-ids`, 'unknown') AS promotion_ids,
    COALESCE(`Order ID`, 'unknown') AS order_id,
    COALESCE(CAST(Date AS STRING), 'unknown') AS date,
    COALESCE(Category, 'unknown') AS category,
    COALESCE(currency, 'unknown') AS currency,
    COALESCE(Amount, 0.0) AS amount,
    COALESCE(B2B, 'unknown') AS b2b
  FROM
    {{ source('dbt_assigment', 'amazon_sale_report') }}
)

SELECT 
  {{ dbt_utils.generate_surrogate_key([
    'promotion_ids',
    'order_id',
    'date',
    'category',
    'currency',
    'CAST(amount AS STRING)',
    'b2b'
  ]) }} AS promotion_id,
  promotion_ids,
  order_id,
  date,
  category,
  currency,
  amount,
  b2b
FROM t_data
