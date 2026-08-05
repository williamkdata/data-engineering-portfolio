SELECT
    ts AS datetime,
    CAST(ts AS DATE) AS date,
    EXTRACT(hour FROM ts) AS heure,
    EXTRACT({{ "dow" if target.name == 'dev' else "DAYOFWEEK" }} FROM ts) AS jour_semaine,
    EXTRACT({{ "dow" if target.name == 'dev' else "DAYOFWEEK" }} FROM ts) IN {{ "(0, 6)" if target.name == 'dev' else "(1, 7)" }} AS est_weekend,
    EXTRACT(month FROM ts) AS mois
FROM {{ "generate_series(TIMESTAMP '2026-07-01 00:00:00',TIMESTAMP '2026-12-31 23:00:00',INTERVAL '1 hour') AS t(ts)" if target.name == 'dev' 
        else "UNNEST(GENERATE_TIMESTAMP_ARRAY(TIMESTAMP '2026-07-01 00:00:00', TIMESTAMP '2026-12-31 23:00:00', INTERVAL 1 HOUR)) AS ts" }} 
