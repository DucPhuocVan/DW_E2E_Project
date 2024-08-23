{{ config(
    materialized='table',
    schema='report'
) }}
SELECT
	date.year,
	date.month,
	region.region_name,
	round(SUM(sale.order_revenue)) as revenue,
	round(SUM(sale.order_revenue - sale.order_cost)) as profit
FROM {{ ref('fact_sale') }} sale
LEFT JOIN {{ ref('dim_date') }} date
	ON sale.order_date = date.d_date 
LEFT JOIN {{ ref('dim_region') }} region
	ON sale.region_code = region.region_code
GROUP BY 
	date.year,
	date.month,
	region.region_name
ORDER BY 
	date.year,
	date.month,
	region.region_name