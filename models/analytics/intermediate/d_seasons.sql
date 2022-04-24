select
    /* Primary Key */
    seasons.id
    /* Seasons Properties */
    , seasons.regular_season_start_date
    , seasons.regular_season_end_date
    , seasons.season_end_date
    , seasons.number_of_games
    , seasons.has_ties_in_use
    , seasons.has_olympics_participation
    , seasons.has_conferences_in_use
    , seasons.has_divisions_in_use
    , seasons.has_wildcard_in_use
from 
    {{ ref('stg_nhl__seasons') }} as seasons
