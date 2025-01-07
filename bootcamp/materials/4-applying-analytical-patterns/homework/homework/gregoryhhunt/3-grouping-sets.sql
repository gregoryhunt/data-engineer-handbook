WITH grouped_data AS (
    SELECT COALESCE(gd.team_abbreviation, 'overall')   as team,
         COALESCE(gd.player_name, 'overall')         as player,
         SUM(COALESCE(gd.pts, 0))                       points,
         COALESCE(CAST(g.season AS TEXT), 'overall') as season,
         COUNT(DISTINCT gd.game_id) as games_played,
        SUM(CASE
            WHEN gd.team_id = g.home_team_id AND home_team_wins = 1 THEN 1
            WHEN gd.team_id = g.visitor_team_id AND home_team_wins = 0 THEN 1
            ELSE 0
        END) as total_wins
    FROM game_details gd
           JOIN games g on g.game_id = gd.game_id
    GROUP BY GROUPING SETS ( (gd.player_name, gd.team_abbreviation),
                           (gd.player_name, g.season),
                           (gd.team_abbreviation, g.game_id)
    )
),
highest_player_team as (
    SELECT *
    FROM grouped_data
    WHERE player != 'overall'
      AND team != 'overall'
    ORDER BY points DESC
    LIMIT 1
),
highest_player_season as (
    SELECT *
    FROM grouped_data
    WHERE player != 'overall'
      AND season != 'overall'
    ORDER BY points DESC
    LIMIT 1
),
team_most_wins as (
    SELECT *
    FROM grouped_data
    WHERE team != 'overall'
      AND player = 'overall'
      AND season = 'overall'
      AND games_played > 0  -- Ensure we only get team-level aggregates
    ORDER BY total_wins DESC
    LIMIT 1
)
SELECT * FROM highest_player_team
UNION ALL
SELECT * FROM highest_player_season
UNION ALL
SELECT * FROM team_most_wins;