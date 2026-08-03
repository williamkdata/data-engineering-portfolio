{{config(severity='warn')}}

SELECT 
    si.station_id,
    si.name,
    ss.num_bikes_available,
    si.capacity
FROM {{ ref('stg_station_information') }} si
LEFT JOIN {{ ref('stg_station_status') }} ss
    ON si.station_id = ss.station_id
WHERE ss.num_bikes_available < 0 or ss.num_bikes_available > si.capacity