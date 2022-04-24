select
    /* Primary Key */
    dates.date_id
    /* Date Properties */
    , dates.date_day
    , dates.day_of_week_name
    , dates.quarter_number
    , dates.quarter_desc
    , dates.month_number
    , dates.month_name
    , dates.month_desc
    , dates.week_number
    , dates.week_desc
from 
    {{ ref('dates') }} as dates
