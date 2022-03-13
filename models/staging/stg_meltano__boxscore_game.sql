
with

live_boxscore as (
    select * from {{ source('meltano', 'live_boxscore') }}
),

-- CTE1
boxscore_game as (
    select
        /* Primary Key */
        live_boxscore.game_id as id

        /* Foreign Keys */
        , live_boxscore.game_id
        , teams.home.team.id as home_team_id
        , teams.away.team.id as away_team_id

        /* Properties */
        , concat(teams.home.team.name, ' (Home) vs ', teams.away.team.name, ' (Away)') as game_matchup
        , case 
            when teams.home.teamStats.teamSkaterStats.goals  > teams.away.teamStats.teamSkaterStats.goals  then teams.home.team.id
            when teams.home.teamStats.teamSkaterStats.goals  < teams.home.teamStats.teamSkaterStats.goals  then teams.away.team.id
            else NULL
        end as game_winning_team
        , case 
            when teams.home.teamStats.teamSkaterStats.goals  > teams.away.teamStats.teamSkaterStats.goals  then 'Home'
            when teams.home.teamStats.teamSkaterStats.goals  < teams.home.teamStats.teamSkaterStats.goals  then 'Away'
            else NULL
        end as game_winning_team_type
        , ABS(home_team.team_goals - away_team.team_goals) as absolute_goal_differential

        /* Home team stats*/
        , teams.home.team.name as home_team_name
        , teams.home.teamStats.teamSkaterStats.goals as home_team_goals
        , teams.home.teamStats.teamSkaterStats.pim as home_team_pim
        , teams.home.teamStats.teamSkaterStats.shots as home_team_shots
        , teams.home.teamStats.teamSkaterStats.powerPlayGoals as home_team_powerplay_goals
        , teams.home.teamStats.teamSkaterStats.powerPlayOpportunities as home_team_powerplay_opportunities
        , teams.home.teamStats.teamSkaterStats.faceOffWinPercentage as home_team_faceoff_percentage
        , teams.home.teamStats.teamSkaterStats.blocked as home_team_blocked
        , teams.home.teamStats.teamSkaterStats.takeaways as home_team_takeaways
        , teams.home.teamStats.teamSkaterStats.giveaways as home_team_giveaways
        , teams.home.teamStats.teamSkaterStats.hits as home_team_hits

        /* Away team stats*/
        , teams.away.team.name as away_team_name
        , teams.away.teamStats.teamSkaterStats.goals as away_team_goals
        , teams.away.teamStats.teamSkaterStats.pim as away_team_pim
        , teams.away.teamStats.teamSkaterStats.shots as away_team_shots
        , teams.away.teamStats.teamSkaterStats.powerPlayGoals as away_team_powerplay_goals
        , teams.away.teamStats.teamSkaterStats.powerPlayOpportunities as away_team_powerplay_opportunities
        , teams.away.teamStats.teamSkaterStats.faceOffWinPercentage as away_team_faceoff_percentage
        , teams.away.teamStats.teamSkaterStats.blocked as away_team_blocked
        , teams.away.teamStats.teamSkaterStats.takeaways as away_team_takeaways
        , teams.away.teamStats.teamSkaterStats.giveaways as away_team_giveaways
        , teams.away.teamStats.teamSkaterStats.hits as away_team_hits

    from
        live_boxscore
        )

-- Final query, return everything
select  
    /* Primary Key */
    boxscore_game.id

    /* Foreign Keys */
    , boxscore_game.home_team_id
    , boxscore_game.away_team_id

    /* Properties */
     , boxscore_game.game_matchup
     , boxscore_game.gamewinning_team_id
     , boxscore_game.game_winning_team_type
     , boxscore_game.game_winning_team_type

    /* Home team stats*/
    , boxscore_game.home_team_name
    , boxscore_game.home_team_goals
    , boxscore_game.home_team_pim
    , boxscore_game.home_team_shots
    , boxscore_game.home_team_powerplay_goals
    , boxscore_game.home_team_powerplay_opportunities
    , boxscore_game.home_team_faceoff_percentage
    , boxscore_game.home_team_blocked
    , boxscore_game.home_team_takeaways
    , boxscore_game.home_team_giveaways
    , boxscore_game.home_team_hits

    /* Away team stats*/
    , boxscore_game.away_team_name
    , boxscore_game.away_team_goals
    , boxscore_game.away_team_pim
    , boxscore_game.away_team_shots
    , boxscore_game.away_team_powerplay_goals
    , boxscore_game.away_team_powerplay_opportunities
    , boxscore_game.away_team_faceoff_percentage
    , boxscore_game.away_team_blocked
    , boxscore_game.away_team_takeaways
    , boxscore_game.away_team_giveaways
    , boxscore_game.away_team_hits

from     
    boxscore_game

order by
    boxscore_game.game_id desc
