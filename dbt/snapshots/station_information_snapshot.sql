{% snapshot station_information_snapshot %}

{{
    config(
        schema='snapshots',
        strategy='check',
        unique_key='station_id',
        check_cols=['capacity', 'name'],
    )
}}

select * from {{ source('velib', 'station_information') }}

{% endsnapshot %}
