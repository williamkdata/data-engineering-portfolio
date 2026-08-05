{{config(severity='error')}}

SELECT station_id, COUNT(*) AS nb_lignes_courantes
FROM {{ ref('dim_station') }}
WHERE dbt_valid_to IS NULL
GROUP BY station_id
HAVING COUNT(*) > 1
