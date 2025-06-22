{{
  config(
    unique_key='sk_airport_id'
  )
}}

with deduplicated_airports as (
  select 
    airport_id as nk_airport_id,
    airport_name,
    city,
    latitude,
    longitude,
    -- Add any timestamp column if available for versioning:
    -- updated_at,
    row_number() over (
      partition by airport_id 
      order by airport_id  -- or use updated_at DESC if available
    ) as rn
  from {{ ref('stg_airports') }}
),

final_dim_airports as (
  select
    {{ dbt_utils.generate_surrogate_key([
        'nk_airport_id',
        'airport_name',
        'city',
        'latitude',
        'longitude'
    ]) }} as sk_airport_id,
    nk_airport_id,
    airport_name,
    city,
    latitude,
    longitude,
    {{ dbt_date.now() }} as created_at,
    {{ dbt_date.now() }} as updated_at
  from deduplicated_airports
  where rn = 1  -- Ensures one record per airport
)

select * from final_dim_airports