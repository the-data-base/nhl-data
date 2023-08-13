select
    /* Primary Key */
    season_id

    /* Properties */
    , regular_season_start_date
    , regular_season_end_date
    , season_end_date
    , number_of_games
    , has_ties_in_use
    , has_olympics_participation
    , has_conferences_in_use
    , has_divisions_in_use
    , has_wildcard_in_use
from {{ ref('stg_nhl__seasons') }}
