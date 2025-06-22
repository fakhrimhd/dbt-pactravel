{{
  config(
    unique_key='sk_customer_id'
  )
}}

with deduplicated_customers as (
  select 
    customer_id as nk_customer_id,
    customer_first_name,
    customer_family_name,
    customer_gender,
    customer_country,
    -- Add any timestamp column if available for versioning:
    -- updated_at,
    row_number() over (
      partition by customer_id
      order by customer_id  -- or use updated_at DESC if available
    ) as rn
  from {{ ref('stg_customers') }}
),

final_dim_customers as (
  select
    {{ dbt_utils.generate_surrogate_key([
        'nk_customer_id',
        'customer_first_name',
        'customer_family_name',
        'customer_country'
    ]) }} as sk_customer_id,
    nk_customer_id,
    customer_first_name,
    customer_family_name,
    customer_gender,
    customer_country,
    {{ dbt_date.now() }} as created_at,
    {{ dbt_date.now() }} as updated_at
  from deduplicated_customers
  where rn = 1  -- Ensures one record per customer
)

select * from final_dim_customers