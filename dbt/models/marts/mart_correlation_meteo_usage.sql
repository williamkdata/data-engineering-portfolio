WITH meteo_dedup AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY time ORDER BY ingested_at ASC) AS rang
    FROM {{ ref('stg_weather') }}
    QUALIFY rang = 1
)
select  ss.station_id,
        ss.ingested_at,
        ss.num_bikes_available,
        w.temperature_2m,
        w.precipitation,
        CONCAT(ss.station_id, '_', CAST(ss.ingested_at AS VARCHAR)) AS station_snapshot_id
from {{ ref('stg_station_status') }} ss
LEFT JOIN meteo_dedup w ON (ss.snapshot_hour = w.time)