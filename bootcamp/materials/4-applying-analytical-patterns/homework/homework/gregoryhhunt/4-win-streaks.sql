ITH all_teams_games AS (
    SELECT
        gd.team_id,
        gd.game_id,
        gd.team_abbreviation,
        g.game_date_est,
         CASE
            WHEN (gd.team_id = g.home_team_id AND g.home_team_wins = 1) OR
                 (gd.team_id = g.visitor_team_id AND g.home_team_wins = 0)
                THEN 1
            ELSE 0
            END AS won_game
    FROM game_details gd
    LEFT JOIN games g
    ON gd.game_id = g.game_id
    GROUP BY 1, 2, 3, 4, 5
),
rolling_wins AS (
    SELECT
        team_abbreviation,
        game_date_est,
        SUM(won_game) OVER (
            PARTITION BY team_id
            ORDER BY game_date_est
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) as wins_in_90_games,
        COUNT(*) OVER (
            PARTITION BY team_id
            ORDER BY game_date_est
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) as games_in_window
    FROM all_teams_games
)
SELECT
    team_abbreviation,
    game_date_est,
    wins_in_90_games
FROM rolling_wins rw
WHERE games_in_window = 90
ORDER BY wins_in_90_games DESC
LIMIT 1;