SELECT
    salesterritorykey AS region_code,
    region AS region_name,
    country AS country,
    continent AS continent
FROM {{ source('stg', 'territories')}}