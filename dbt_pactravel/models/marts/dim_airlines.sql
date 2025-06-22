{{
  config(
    unique_key='sk_airline_id'
  )
}}

with deduplicated_airlines as (
  select 
    airline_id as nk_airline_id,
    airline_name,
    country,
    airline_icao as icao_code,
    -- Add any timestamp column if available for proper versioning:
    -- updated_at,
    row_number() over (
      partition by airline_id 
      order by airline_id  -- or use updated_at DESC if available
    ) as rn
  from {{ ref('stg_airlines') }}
),

final_dim_airlines as (
  select
    {{ dbt_utils.generate_surrogate_key([
        'nk_airline_id',
        'icao_code',
        'airline_name'
    ]) }} as sk_airline_id,
    nk_airline_id,
    airline_name,
    country,
    icao_code,
    {{ dbt_date.now() }} as created_at,
    {{ dbt_date.now() }} as updated_at
  from deduplicated_airlines
  where rn = 1  -- Ensures one record per airline
)

select * from final_dim_airlines