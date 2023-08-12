select
  /* Primary Key */
  date_id

  /* Properties */
  , date_day
  , day_of_week_name
  , quarter_number
  , quarter_desc
  , month_number
  , month_name
  , month_desc
  , week_number
  , week_desc
from {{ ref('dates') }}
