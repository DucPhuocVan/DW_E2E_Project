SELECT
    CAST(date AS DATE) AS d_date,
    EXTRACT(MONTH FROM (CAST(date AS DATE))) AS month,
    EXTRACT(YEAR FROM (CAST(date AS DATE))) AS year
FROM {{ source('stg', 'calendar')}}