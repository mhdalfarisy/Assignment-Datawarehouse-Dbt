-- dim_sales_order
{{
  config(
    materialized='table'
  )
}}

WITH t_data AS (
  SELECT DISTINCT 
    COALESCE(`Order ID`, 'unknown') AS order_id,
    COALESCE(CAST(Date AS STRING), 'unknown') AS date,
    COALESCE(Status, 'unknown') AS status,
    COALESCE(Fulfilment, 'unknown') AS fulfilment,
    COALESCE(`Sales Channel`, 'unknown') AS sales_channel,
    COALESCE(Qty, 0) AS qty,
    COALESCE(currency, 'unknown') AS currency,
    COALESCE(Amount, 0.0) AS amount
  FROM
    {{ source('dbt_assigment', 'amazon_sale_report') }}
)

SELECT 
  {{ dbt_utils.generate_surrogate_key([
    'order_id',
    'date',
    'status',
    'fulfilment',
    'sales_channel',
    'CAST(qty AS STRING)',
    'currency',
    'CAST(amount AS STRING)'
  ]) }} AS sales_order_id,
  order_id,
  date,
  status,
  fulfilment,
  sales_channel,
  qty,
  currency,
  amount
FROM t_data
