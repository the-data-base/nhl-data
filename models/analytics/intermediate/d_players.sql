WITH

player_season as (
    SELECT  
        players.*
        ,FORMAT_TIMESTAMP("%Y", TIMESTAMP (players._time_extracted)) AS year
        ,FORMAT_TIMESTAMP("%m", TIMESTAMP (players._time_extracted)) AS month
        ,CASE 
            WHEN CAST(FORMAT_TIMESTAMP("%m", TIMESTAMP (players._time_extracted)) AS INT64) < 10
                THEN CAST(FORMAT_TIMESTAMP("%Y", TIMESTAMP (players._time_extracted)) AS INT64) - 1
            ELSE CAST(FORMAT_TIMESTAMP("%Y", TIMESTAMP (players._time_extracted)) AS INT64)
            END as season_start
        ,CASE 
            WHEN CAST(FORMAT_TIMESTAMP("%m", TIMESTAMP (players._time_extracted)) AS INT64) < 10
                THEN CAST(FORMAT_TIMESTAMP("%Y", TIMESTAMP (players._time_extracted)) AS INT64) - 1 + 1
            ELSE CAST(FORMAT_TIMESTAMP("%Y", TIMESTAMP (players._time_extracted)) AS INT64) + 1
            END as season_end
    FROM 
        {{ ref('stg_meltano__players') }} as players
)

SELECT 
    /* Primary Key */
    p.id

    /* Foreign Keys */
    ,CONCAT(p.season_start, p.season_end) as season_id

    /* Properties */
    ,p.fullName as player_full_name
    ,p.firstName as player_first_name
    ,p.lastName as player_last_name
    ,PARSE_DATE('%Y-%m-%d',  p.birthDate) as player_birth_date
    ,DATE_DIFF(CURRENT_DATE(),PARSE_DATE('%Y-%m-%d',  p.birthDate), YEAR) AS player_age_years
    ,DATE_DIFF(CURRENT_DATE(),PARSE_DATE('%Y-%m-%d',  p.birthDate), DAY) AS player_age_days
    ,p.birthCity as player_birth_city
    ,p.birthStateProvince as player_birth_state_province
    ,p.birthCountry as player_birth_country
    ,p.nationality as player_nationality
    ,p.height as player_height
    ,p.weight as player_weight
    ,p.shootsCatches as player_shoots_catches
    ,p.primaryPosition.name as prospect_position_name
    ,p.primaryPosition.abbreviation as prospect_position_abbreviation
    ,CASE 
        WHEN p.rosterStatus = 'Y' 
            THEN 'true'
        ELSE 
            'false'
        END as is_currently_rostered
    ,p.active as is_active
    ,p.rookie as is_rookie
    ,p.alternateCaptain as is_captain
    ,p.alternateCaptain as is_alternate_captain    
    ,FORMAT_TIMESTAMP("%Y-%m-%d", TIMESTAMP (p._time_extracted)) AS date_extracted
    ,p.currentTeam.name as player_current_team_name

FROM 
    player_season p