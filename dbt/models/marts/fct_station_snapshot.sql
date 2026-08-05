{% if target.name == 'prod' %}

{{ config(
    partition_by={"field": "ingested_at", "data_type": "timestamp", "granularity": "day"},cluster_by=["station_id"]
) }}

{% endif %}

SELECT
    v.station_id,
    v.ingested_at,
    v.num_bikes_available,
    v.num_docks_available,
    v.variation,
    s.dbt_scd_id AS station_scd_id,
    t.datetime AS temps_id,
    CONCAT(v.station_id, '_', {{ "CAST(v.ingested_at AS VARCHAR)" if target.name == 'dev' else "CAST(v.ingested_at AS STRING)" }}) AS station_snapshot_id
FROM {{ ref('int_station_variation') }} v
LEFT JOIN {{ ref('dim_station') }} s
    ON v.station_id = s.station_id
    AND v.ingested_at >= s.valid_from_effective
    AND (s.dbt_valid_to IS NULL OR v.ingested_at < s.dbt_valid_to)
LEFT JOIN {{ ref('dim_temps') }} t
    ON v.snapshot_hour = t.datetime
