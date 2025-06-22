select *
from {{ source('dwh_pactravel', 'hotel_bookings') }}