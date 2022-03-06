SELECT 
    /* Primary Key */
    p.id

    /* Foreign Keys */
    ,p.nhlPlayerId as prospect_player_id
    ,p.prospectCategory.id as prospect_category_id
    
    /* Properties */
    ,p.firstName as prospect_first_name
    ,p.lastName as prospect_last_name
    ,p.fullName as prospect_full_name
    ,PARSE_DATE('%Y-%m-%d',  p.birthDate) as prospect_birth_date
    ,DATE_DIFF(CURRENT_DATE(),PARSE_DATE('%Y-%m-%d',  p.birthDate), YEAR) AS prospect_age_years
    ,DATE_DIFF(CURRENT_DATE(),PARSE_DATE('%Y-%m-%d',  p.birthDate), DAY) AS prospect_age_days
    ,p.birthCity as prospect_birth_city
    ,p.birthStateProvince as prospect_birth_state_province
    ,p.birthCountry as prospect_birth_country
    ,p.height as prospect_height
    ,p.weight as prospect_weight
    ,p.shootsCatches as prospect_shoots_catches
    ,p.primaryPosition.name as prospect_position_name
    ,p.primaryPosition.abbreviation as prospect_position_abbreviation
    ,p.draftStatus as prospect_draft_status -- wtf is this?
    ,p.prospectCategory.name as prospect_category_name 
    ,p.prospectCategory.shortName as prospect_category_short_name
    ,p.amateurTeam.name as prospect_amateur_team_name
    ,p.amateurTeam.link as prospect_amateur_team_url
    ,p.amateurLeague.name as prospect_amateur_league_name
    ,p.amateurLeague.link as prospect_amateur_league_url
    ,p.ranks.midterm as prospect_rank_midterm
    ,p.ranks.draftYear as prospect_rank_draft_year
    ,p.link as prospect_url
FROM 
    {{ source('meltano', 'draft_prospects') }}
ORDER BY
    PARSE_DATE('%Y-%m-%d',  p.birthDate)  desc