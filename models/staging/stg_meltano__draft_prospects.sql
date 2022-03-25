select 
    /* Primary Key */
    distinct prospects.id

    /* Foreign Keys */
    ,prospects.nhlPlayerId as prospect_player_id
    ,prospects.prospectCategory.id as prospect_category_id
    
    /* Properties */
    ,prospects.firstName as prospect_first_name
    ,prospects.lastName as prospect_last_name
    ,prospects.fullName as prospect_full_name
    ,PARSE_DATE('%Y-%m-%d',  prospects.birthDate) as prospect_birth_date
    ,DATE_DIFF(CURRENT_DATE(),PARSE_DATE('%Y-%m-%d',  prospects.birthDate), YEAR) AS prospect_age_years
    ,DATE_DIFF(CURRENT_DATE(),PARSE_DATE('%Y-%m-%d',  prospects.birthDate), DAY) AS prospect_age_days
    ,prospects.birthCity as prospect_birth_city
    ,prospects.birthStateProvince as prospect_birth_state_province
    ,prospects.birthCountry as prospect_birth_country
    ,prospects.height as prospect_height
    ,prospects.weight as prospect_weight
    ,prospects.shootsCatches as prospect_shoots_catches
    ,prospects.primaryPosition.name as prospect_position_name
    ,prospects.primaryPosition.abbreviation as prospect_position_abbreviation
    ,prospects.draftStatus as prospect_draft_status -- wtf is this?
    ,prospects.prospectCategory.name as prospect_category_name 
    ,prospects.prospectCategory.shortName as prospect_category_short_name
    ,prospects.amateurTeam.name as prospect_amateur_team_name
    ,prospects.amateurTeam.link as prospect_amateur_team_url
    ,prospects.amateurLeague.name as prospect_amateur_league_name
    ,prospects.amateurLeague.link as prospect_amateur_league_url
    ,prospects.ranks.midterm as prospect_rank_midterm
    ,prospects.ranks.draftYear as prospect_rank_draft_year
    ,prospects.link as prospect_url
from 
    {{ source('meltano', 'draft_prospects') }} as prospects