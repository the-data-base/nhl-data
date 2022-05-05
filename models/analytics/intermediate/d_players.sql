with deduplicated as (
    {{ dedupe(
        ref('stg_nhl__players'),
        key_fields=['player_id'],
        sort_fields=['season_id']
    ) }}
)

select
    /* Primary Key */
    player_id

    /* Properties */
    , full_name
    , player_url
    , first_name
    , last_name
    , primary_number
    , birth_date
    , birth_city
    , birth_state_province
    , birth_country
    , nationality
    , height
    , weight
    , is_active
    , is_alternate_captain
    , is_captain
    , is_rookie
    , shoots_catches
    , roster_status
    , primary_position_code
    , primary_position_name
    , primary_position_type
    , primary_position_abbreviation
    , extracted_at
    , loaded_at
from deduplicated

