SELECT station_id,
station_code,
name,
lat,
lon,
capacity,
dbt_scd_id,
CASE
    WHEN dbt_valid_from = MIN(dbt_valid_from) OVER (PARTITION BY station_id)
    THEN TIMESTAMP '1900-01-01'
    ELSE dbt_valid_from
END AS valid_from_effective,
dbt_valid_from,
dbt_valid_to
FROM {{ ref('station_information_snapshot') }}