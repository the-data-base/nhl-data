   select 
        calendar_date
        , format_datetime('%Y%m%d', calendar_date) as date_id
        , extract(year from calendar_date) as year_number
        , extract(quarter from calendar_date) as quarter_number
        , extract(month from calendar_date) as month_number
        , extract(week from calendar_date) as week_number
        , format_datetime('%b', calendar_date) as month_name
        , format_datetime('%a', calendar_date) as day_of_week_name
    from unnest(generate_date_array('2000-01-01', '2099-12-31', interval 1 day)) AS calendar_date
)

, dim_date as (
    select
        date_id
        , calendar_date
        , day_of_week_name
        , quarter_number
        , concat("Q", quarter_number, ' ', year_number) as quarter_desc
        , month_number
        , month_name
        , concat("M", lpad(cast(month_number as string), 2, '0'), ' ', year_number) as month_desc
        , week_number
        , concat("Wk ", lpad(cast(week_number as string), 2, '0'), ' ', year_number) as week_desc
    from raw_dates
)

select * from dim_date