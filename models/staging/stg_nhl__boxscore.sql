with
live_boxscore as (
    select * from {{ source('meltano', 'live_boxscore') }}
)

, final as (
    select
        /* Primary Key */
        {{ dbt_utils.surrogate_key(['gameid']) }} as id

        /* Foreign Keys */
        , gameid as game_id
        , teams.home.team.id as home_team_id
        , teams.away.team.id as away_team_id

        /* Properties */
        -- Home team stats
        , teams.home.team.name as home_team_name
        , teams.home.teamstats.teamskaterstats.pim as home_team_pim
        , teams.home.teamstats.teamskaterstats.shots as home_team_shots
        , teams.home.teamstats.teamskaterstats.powerplaygoals as home_team_powerplay_goals
        , teams.home.teamstats.teamskaterstats.powerplayopportunities as home_team_powerplay_opportunities
        , teams.home.teamstats.teamskaterstats.faceoffwinpercentage as home_team_faceoff_percentage
        , teams.home.teamstats.teamskaterstats.blocked as home_team_blocked
        , teams.home.teamstats.teamskaterstats.takeaways as home_team_takeaways
        , teams.home.teamstats.teamskaterstats.giveaways as home_team_giveaways
        , teams.home.teamstats.teamskaterstats.hits as home_team_hits

        -- Away team stats
        , teams.away.team.name as away_team_name
        , teams.away.teamstats.teamskaterstats.pim as away_team_pim
        , teams.away.teamstats.teamskaterstats.shots as away_team_shots
        , teams.away.teamstats.teamskaterstats.powerplaygoals as away_team_powerplay_goals
        , teams.away.teamstats.teamskaterstats.powerplayopportunities as away_team_powerplay_opportunities
        , teams.away.teamstats.teamskaterstats.faceoffwinpercentage as away_team_faceoff_percentage
        , teams.away.teamstats.teamskaterstats.blocked as away_team_blocked
        , teams.away.teamstats.teamskaterstats.takeaways as away_team_takeaways
        , teams.away.teamstats.teamskaterstats.giveaways as away_team_giveaways
        , teams.away.teamstats.teamskaterstats.hits as away_team_hits
    from live_boxscore
)

select * from final
