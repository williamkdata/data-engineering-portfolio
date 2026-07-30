SELECT
	station_id,
	is_installed,
	is_renting,
	snapshot_hour,
	AVG(num_bikes_available) AS avg_bikes_available,
	AVG(num_docks_available) AS avg_docks_available,
	MAX(num_bikes_available) AS max_bikes_available,
	MAX(num_docks_available) AS max_docks_available
FROM {{ ref('stg_station_status') }} ss
GROUP BY
	station_id,
	is_installed,
	is_renting,
	snapshot_hour