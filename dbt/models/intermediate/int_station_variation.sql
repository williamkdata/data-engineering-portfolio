SELECT
    station_id,
    snapshot_hour,
    num_bikes_available,
    LAG(num_bikes_available) OVER (PARTITION BY station_id ORDER BY snapshot_hour, ingested_at) AS bikes_snapshot_precedent,
    num_bikes_available - LAG(num_bikes_available) OVER (PARTITION BY station_id ORDER BY snapshot_hour, ingested_at) AS variation
FROM {{ ref('stg_station_status') }}
ORDER BY station_id, snapshot_hour