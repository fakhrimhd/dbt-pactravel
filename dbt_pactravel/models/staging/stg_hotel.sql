select *
from {{ source('dwh_pactravel', 'hotel') }}