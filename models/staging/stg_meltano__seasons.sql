select
    /* Primary Key */
    seasons.seasonId as id

    /* Properties */
    , seasons.regularSeasonStartDate as regular_season_start_date
    , seasons.regularSeasonEndDate as regular_season_end_date
    , seasons.seasonEndDate as season_end_date
    , seasons.numberOfGames as number_of_games
    , seasons.tiesInUse as has_ties_in_use
    , seasons.olympicsParticipation as has_olympics_participation
    , seasons.conferencesInUse as has_conferences_in_use
    , seasons.divisionsInUse as has_divisions_in_use
    , seasons.wildCardInUse as has_wildcard_in_use
    , seasons._time_extracted as extracted_at
    , seasons._time_loaded as loaded_at
from {{ source('meltano', 'seasons') }} as seasons
