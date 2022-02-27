select
    /* Primary Key */
    seasonId as id

    /* Properties */
    , regularSeasonStartDate as regular_season_start_date
    , regularSeasonEndDate as regular_season_end_date
    , seasonEndDate as season_end_date
    , numberOfGames as number_of_games
    , tiesInUse as has_ties_in_use
    , olympicsParticipation as has_olympics_participation
    , conferencesInUse as has_conferences_in_use
    , divisionsInUse as has_divisions_in_use
    , wildCardInUse as has_wildcard_in_use
    , _time_extracted as extracted_at
    , _time_loaded as loaded_at
from {{ source('meltano', 'seasons') }}
