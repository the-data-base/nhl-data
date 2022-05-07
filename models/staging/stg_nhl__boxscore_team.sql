with

-- CTE1
live_boxscore as (
    select * from {{ source('meltano', 'live_boxscore') }}
)

-- CTE2
, home_team as (
    select
        /* Identifiers */
        live_boxscore.gameid as game_id
        , teams.home.team.id as team_id

        /* Properties */
        , 'Home' as team_type

        /* Team stats*/
        , teams.home.team.name as team_name
        , teams.home.teamstats.teamskaterstats.goals as team_goals
        , teams.home.teamstats.teamskaterstats.pim as team_pim
        , teams.home.teamstats.teamskaterstats.shots as team_shots
        , teams.home.teamstats.teamskaterstats.powerplaygoals as team_powerplay_goals
        , teams.home.teamstats.teamskaterstats.powerplayopportunities as team_powerplay_opportunities
        , teams.home.teamstats.teamskaterstats.faceoffwinpercentage as team_faceoff_percentage
        , teams.home.teamstats.teamskaterstats.blocked as team_blocked
        , teams.home.teamstats.teamskaterstats.takeaways as team_takeaways
        , teams.home.teamstats.teamskaterstats.giveaways as team_giveaways
        , teams.home.teamstats.teamskaterstats.hits as team_hits

    from live_boxscore
)

-- CTE2
, away_team as (
    select
        /* Identifiers */
        live_boxscore.gameid as game_id
        , teams.away.team.id as team_id

        /* Properties */
        , 'Away' as team_type

        /* Team stats*/
        , teams.away.team.name as team_name
        , teams.away.teamstats.teamskaterstats.goals as team_goals
        , teams.away.teamstats.teamskaterstats.pim as team_pim
        , teams.away.teamstats.teamskaterstats.shots as team_shots
        , teams.away.teamstats.teamskaterstats.powerplaygoals as team_powerplay_goals
        , teams.away.teamstats.teamskaterstats.powerplayopportunities as team_powerplay_opportunities
        , teams.away.teamstats.teamskaterstats.faceoffwinpercentage as team_faceoff_percentage
        , teams.away.teamstats.teamskaterstats.blocked as team_blocked
        , teams.away.teamstats.teamskaterstats.takeaways as team_takeaways
        , teams.away.teamstats.teamskaterstats.giveaways as team_giveaways
        , teams.away.teamstats.teamskaterstats.hits as team_hits

    from live_boxscore
)

-- CTE4
, winning_team as (
    select
        home_team.game_id
        , home_team.team_id as home_team_id
        , away_team.team_id as away_team_id
        , home_team.team_goals as home_team_score
        , away_team.team_goals as away_team_score
        , case
            when home_team.team_goals > away_team.team_goals then 'Home'
            when home_team.team_goals < away_team.team_goals then 'Away'
        end as winning_team
        , abs(home_team.team_goals - away_team.team_goals) as absolute_goal_differential
    from home_team
    inner join away_team on home_team.game_id = away_team.game_id

)

-- CTE5
, boxscore_team as (
    select * from home_team
    union all
    select * from away_team

)

-- Final query, return everything
select
    /* Primary Key */
    {{ dbt_utils.surrogate_key(['boxscore_team.game_id', 'boxscore_team.team_id']) }} as stg_nhl__boxscore_team_id

    /* Identifiers */
    , boxscore_team.game_id
    , boxscore_team.team_id

    /* Properties */
    , boxscore_team.team_type

    /* Team stats*/
    , boxscore_team.team_name
    , case
        when boxscore_team.team_type = winning_team.winning_team then "true"
        when boxscore_team.team_type <> winning_team.winning_team then "false"
    end as team_winner
    , boxscore_team.team_goals
    , case
        when boxscore_team.team_type = winning_team.winning_team then winning_team.absolute_goal_differential
        when boxscore_team.team_type <> winning_team.winning_team then winning_team.absolute_goal_differential * -1
    end as team_goal_differential
    , boxscore_team.team_pim
    , boxscore_team.team_shots
    , boxscore_team.team_powerplay_goals
    , boxscore_team.team_powerplay_opportunities
    , boxscore_team.team_faceoff_percentage
    , boxscore_team.team_blocked
    , boxscore_team.team_takeaways
    , boxscore_team.team_giveaways
    , boxscore_team.team_hits

from boxscore_team
left join winning_team on boxscore_team.game_id = winning_team.game_id

order by
    boxscore_team.game_id desc
