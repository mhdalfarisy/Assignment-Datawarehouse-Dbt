-- dim_sales_summary
{{
  config(
    materialized='table'
  )
}}

WITH t_data AS (
  SELECT DISTINCT 
    COALESCE(CAST(Date AS STRING), 'unknown') AS date,
    COALESCE(Fulfilment, 'unknown') AS fulfilment,
    COALESCE(Qty, 0) AS qty,
    COALESCE(Amount, 0.0) AS amount
  FROM
    {{ source('dbt_assigment', 'amazon_sale_report') }}
)

SELECT 
  {{ dbt_utils.generate_surrogate_key([
    'date',
    'fulfilment',
    'CAST(qty AS STRING)',
    'CAST(amount AS STRING)'
  ]) }} AS sales_summary_id,
  date,
  fulfilment,
  qty,
  amount
FROM t_data
