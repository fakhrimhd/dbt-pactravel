select *
from {{ source('dwh_pactravel', 'flight_bookings') }}