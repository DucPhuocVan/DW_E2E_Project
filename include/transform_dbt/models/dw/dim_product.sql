SELECT
    product.productkey AS product_code,
    product.productname AS product_name,
    product.modelname AS product_model,
    product_sub.subcategoryname AS product_subcategory,
    product_cat.categoryname AS product_category,
    CAST(product.productcost AS FLOAT) AS product_cost,
    CAST(product.productprice AS FLOAT) AS product_price
FROM {{ source('stg', 'products')}} product
LEFT JOIN {{ source('stg', 'product_subcategories')}} product_sub
    ON product.productsubcategorykey = product_sub.productsubcategorykey
LEFT JOIN {{ source('stg', 'product_categories')}} product_cat
    ON product_sub.productcategorykey = product_cat.productcategorykey
