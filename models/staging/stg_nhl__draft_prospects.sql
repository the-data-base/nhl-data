select distinct
    /* Primary Key */
    {{ dbt_utils.surrogate_key(['prospects.id', 'prospects.nhlplayerid']) }} as stg_nhl__draft_prospects_id

    /* Identifiers */
    , prospects.id as draft_prospect_id
    , prospects.nhlplayerid as prospect_player_id
    , prospects.prospectcategory.id as prospect_category_id

    /* Properties */
    , prospects.firstname as prospect_first_name
    , prospects.lastname as prospect_last_name
    , prospects.fullname as prospect_full_name
    , parse_date('%Y-%m-%d', prospects.birthdate) as prospect_birth_date
    , date_diff(current_date(), parse_date('%Y-%m-%d', prospects.birthdate), year) as prospect_age_years
    , date_diff(current_date(), parse_date('%Y-%m-%d', prospects.birthdate), day) as prospect_age_days
    , prospects.birthcity as prospect_birth_city
    , prospects.birthstateprovince as prospect_birth_state_province
    , prospects.birthcountry as prospect_birth_country
    , prospects.height as prospect_height
    , prospects.weight as prospect_weight
    , prospects.shootscatches as prospect_shoots_catches
    , prospects.primaryposition.name as prospect_position_name
    , prospects.primaryposition.abbreviation as prospect_position_abbreviation
    , prospects.draftstatus as prospect_draft_status -- wtf is this?
    , prospects.prospectcategory.name as prospect_category_name
    , prospects.prospectcategory.shortname as prospect_category_short_name
    , prospects.amateurteam.name as prospect_amateur_team_name
    , prospects.amateurteam.link as prospect_amateur_team_url
    , prospects.amateurleague.name as prospect_amateur_league_name
    , prospects.amateurleague.link as prospect_amateur_league_url
    , prospects.ranks.midterm as prospect_rank_midterm
    , prospects.ranks.draftyear as prospect_rank_draft_year
    , prospects.link as prospect_url
from {{ source('meltano', 'draft_prospects') }} as prospects
