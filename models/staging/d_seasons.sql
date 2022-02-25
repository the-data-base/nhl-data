select
    /* Primary Key */
    seasonid as id

    /* Properties */
    , regularseasonstartdate as regular_season_start_date
    , regularseasonenddate as regular_season_end_date
    , seasonenddate as season_end_date
    , numberofgames as number_of_games
    , tiesinuse as has_ties_in_use
    , olympicsparticipation as has_olympics_participation
    , conferencesinuse as has_conferences_in_use
    , divisionsinuse as has_divisions_in_use
    , wildcardinuse as has_wildcard_in_use

from {{ ref('seasons') }}