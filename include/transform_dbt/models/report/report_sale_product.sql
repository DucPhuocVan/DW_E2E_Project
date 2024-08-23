{{ config(
    materialized='table',
    schema='report'
) }}
SELECT
	date.year,
	date.month,
	product.product_category,
	product.product_subcategory,
	product.product_name,
	round(SUM(sale.order_revenue)) as revenue,
	round(SUM(sale.order_revenue - sale.order_cost)) as profit
FROM {{ ref('fact_sale') }} sale
LEFT JOIN {{ ref('dim_date') }} date
	ON sale.order_date = date.d_date 
LEFT JOIN {{ ref('dim_product') }} product
	ON sale.product_code = product.product_code
GROUP BY 
	date.year,
	date.month,
	product.product_category,
	product.product_subcategory,
	product.product_name
ORDER BY 
	date.year,
	date.month,
	product.product_category,
	product.product_subcategory,
	product.product_name