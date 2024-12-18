-- fact_sales_transactions
{{
  config(
    materialized='table'
  )
}}

WITH t_data AS (
  SELECT 
    -- Join with dimension tables to get the necessary keys
    p.promotion_id,
    s.sales_channel_id,
    sh.shipment_id,
    -- Fact table columns
    COALESCE(so.order_id, 'unknown') AS order_id,
    COALESCE(CAST(so.date AS STRING), 'unknown') AS date,
    COALESCE(so.qty, 0) AS qty,
    COALESCE(so.amount, 0.0) AS amount,
    -- You can add more columns here as needed (e.g., fulfilment, category, etc.)
    COALESCE(so.fulfilment, 'unknown') AS fulfilment,
    COALESCE(so.currency, 'unknown') AS currency
  FROM
    {{ source('dbt_assigment', 'amazon_sale_report') }} so

  -- Join with dimension tables to get dimension keys
  LEFT JOIN {{ ref('dim_promotion') }} p ON so.promotion_ids = p.promotion_ids
  LEFT JOIN {{ ref('dim_sales_channel') }} s ON so.sales_channel = s.sales_channel
  LEFT JOIN {{ ref('dim_sales_shipment') }} sh ON so.order_id = sh.order_id
)

SELECT 
  -- Generating surrogate key for fact table
  {{ dbt_utils.generate_surrogate_key([
    'promotion_id', 
    'sales_channel_id', 
    'shipment_id', 
    'order_id', 
    'date', 
    'qty', 
    'amount'
  ]) }} AS fact_sales_id,
  
  promotion_id,
  sales_channel_id,
  shipment_id,
  order_id,
  date,
  qty,
  amount,
  fulfilment,
  currency
FROM t_data
