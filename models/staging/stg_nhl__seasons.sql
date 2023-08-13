select
    /* Primary Key */
    {{ dbt_utils.surrogate_key(['seasons.seasonid']) }} as stg_nhl__seasons_id

    /* Identifiers */
    , seasons.seasonid as season_id

    /* Properties */
    , seasons.regularseasonstartdate as regular_season_start_date
    , seasons.regularseasonenddate as regular_season_end_date
    , seasons.seasonenddate as season_end_date
    , seasons.numberofgames as number_of_games
    , seasons.tiesinuse as has_ties_in_use
    , seasons.olympicsparticipation as has_olympics_participation
    , seasons.conferencesinuse as has_conferences_in_use
    , seasons.divisionsinuse as has_divisions_in_use
    , seasons.wildcardinuse as has_wildcard_in_use
    , seasons._time_extracted as extracted_at
    , seasons._time_loaded as loaded_at
from {{ source('meltano', 'seasons') }} as seasons
