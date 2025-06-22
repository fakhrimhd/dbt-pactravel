{{ config(
    unique_key='sk_aircraft_id'
) }}

with deduplicated_aircrafts as (
    select 
        aircraft_id as nk_aircraft_id,
        aircraft_name,
        aircraft_icao as icao_code,
        -- Add any other columns,
        row_number() over (
            partition by aircraft_id 
            order by aircraft_id  -- or use a timestamp column if available
        ) as rn
    from {{ ref('stg_aircrafts') }}
),

final_dim_aircrafts as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'nk_aircraft_id',
            'aircraft_name'
        ]) }} as sk_aircraft_id,
        nk_aircraft_id,
        aircraft_name,
        icao_code,
        {{ dbt_date.now() }} as created_at,
        {{ dbt_date.now() }} as updated_at
    from deduplicated_aircrafts
    where rn = 1  -- This ensures only one record per aircraft
)

select * from final_dim_aircrafts