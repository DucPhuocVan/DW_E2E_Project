SELECT
    CAST(sale.orderdate AS DATE) AS order_date,
    sale.productkey AS product_code,
    sale.customerkey AS customer_code,
    sale.territorykey AS region_code,
    SUM(CAST(sale.orderquantity AS INT) * product.product_cost) AS order_cost,
    SUM(CAST(sale.orderquantity AS INT) * product.product_price) AS order_revenue
FROM {{ source('stg', 'sales')}} sale
LEFT JOIN {{ ref('dim_product') }} product
    ON sale.productkey = product.product_code
GROUP BY
    sale.orderdate,
    sale.productkey,
    sale.customerkey,
    sale.territorykey