SELECT
    m.ingested_at,
    {{ "CAST(m.ingested_at AT TIME ZONE 'Europe/Paris' AS DATE)" if target.name == 'dev' else "DATE(DATETIME(m.ingested_at, 'Europe/Paris'))" }} AS date,
    {{ "EXTRACT(hour FROM m.ingested_at AT TIME ZONE 'Europe/Paris')" if target.name == 'dev' else "EXTRACT(HOUR FROM DATETIME(m.ingested_at, 'Europe/Paris'))" }} AS heure,
    {{ "EXTRACT(dow FROM m.ingested_at AT TIME ZONE 'Europe/Paris')" if target.name == 'dev' else "EXTRACT(DAYOFWEEK FROM DATETIME(m.ingested_at, 'Europe/Paris'))" }} AS jour_semaine,
    {{ "EXTRACT(dow FROM m.ingested_at AT TIME ZONE 'Europe/Paris') IN (0, 6)" if target.name == 'dev' else "EXTRACT(DAYOFWEEK FROM DATETIME(m.ingested_at, 'Europe/Paris')) IN (1, 7)" }} AS est_weekend,
    COUNT(DISTINCT m.station_id) AS nb_stations,
    AVG(m.num_bikes_available) AS moyenne_velos_disponibles,
    AVG(m.precipitation) AS precipitation,
    AVG(m.temperature_2m) AS temperature
FROM {{ ref('mart_correlation_meteo_usage') }} m
GROUP BY m.ingested_at, date, heure, jour_semaine, est_weekend
