select *
from {{ source('dwh_pactravel','customers')}}