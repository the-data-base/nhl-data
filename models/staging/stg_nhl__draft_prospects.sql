with

deduped as (
    select * from {{ source('meltano', 'draft_prospects') }}
    qualify row_number() over (
        partition by
            id
            , nhlplayerid
    ) = 1
)

select
    /* Primary Key */
    {{ dbt_utils.surrogate_key(['id', 'nhlplayerid']) }} as stg_nhl__draft_prospects_id

    /* Foreign Keys */
    , id as draft_prospect_id
    , nhlplayerid as prospect_player_id
    , prospectcategory.id as prospect_category_id

    /* Properties */
    , firstname as prospect_first_name
    , lastname as prospect_last_name
    , fullname as prospect_full_name
    , parse_date('%Y-%m-%d', birthdate) as prospect_birth_date
    , date_diff(current_date(), parse_date('%Y-%m-%d', birthdate), year) as prospect_age_years
    , date_diff(current_date(), parse_date('%Y-%m-%d', birthdate), day) as prospect_age_days
    , birthcity as prospect_birth_city
    , birthstateprovince as prospect_birth_state_province
    , birthcountry as prospect_birth_country
    , height as prospect_height
    , weight as prospect_weight
    , shootscatches as prospect_shoots_catches
    , primaryposition.name as prospect_position_name
    , primaryposition.abbreviation as prospect_position_abbreviation
    , draftstatus as prospect_draft_status -- wtf is this?
    , prospectcategory.name as prospect_category_name
    , prospectcategory.shortname as prospect_category_short_name
    , amateurteam.name as prospect_amateur_team_name
    , amateurteam.link as prospect_amateur_team_url
    , amateurleague.name as prospect_amateur_league_name
    , amateurleague.link as prospect_amateur_league_url
    , ranks.midterm as prospect_rank_midterm
    , ranks.draftyear as prospect_rank_draft_year
    , link as prospect_url
from deduped
