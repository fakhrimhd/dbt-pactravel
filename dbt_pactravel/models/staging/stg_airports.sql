select *
from {{ source('dwh_pactravel','airports')}}