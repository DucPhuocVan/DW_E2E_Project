SELECT
    customerkey AS customer_code,
    CONCAT(firstname, ' ', lastname) AS full_name,
    gender,
    emailaddress AS email
FROM {{ source('stg', 'customers')}}