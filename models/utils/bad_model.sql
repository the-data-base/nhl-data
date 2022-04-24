with date_spine as (
{{ dbt_utils.date_spine(
datepart="day",
start_date="cast('2010-01-01' as date)",
end_date="cast('2030-01-01' as date)"
)
}}
)

, date_periods as (
select
date_day
, format_datetime('%Y%m%d', date_day) as date_id
, extract(year from date_day) as year_number
, extract(quarter from date_day) as quarter_number
, extract(month from date_day) as month_number
, extract(week from date_day) as week_number
, format_datetime('%b', date_day) as month_name
, format_datetime('%a', date_day) as day_of_week_name
from date_spine
)

, dim_date as (
select
date_id
, date_day
, day_of_week_name
, quarter_number
, concat("Q", quarter_number, ' ', year_number) as quarter_desc
, month_number
, month_name
, concat("M", lpad(cast(month_number as string), 2, '0'), ' ', year_number) as month_desc
, week_number
, concat("Wk ", lpad(cast(week_number as string), 2, '0'), ' ', year_number) as week_desc
from date_periods
)

select * from dim_date
