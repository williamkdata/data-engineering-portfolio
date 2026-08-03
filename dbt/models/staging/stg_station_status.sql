SELECT 
station_id,
num_bikes_available,
{{"date_trunc('hour', ingested_at)" if target.name =='dev' else 'TIMESTAMP_TRUNC(ingested_at, HOUR)'}} as snapshot_hour,
ingested_at,
num_docks_available,
is_installed,
is_renting
FROM
{{ source('velib', 'station_status') }}