INSERT INTO players_state_tracking
WITH last_season AS (
    SELECT
        *
    FROM players_state_tracking
    WHERE current_season = 1999
),
current_season AS (
    SELECT
        player_name,
        season as current_season,
        COUNT(1)
    FROM player_seasons
    WHERE season = 2000
    GROUP BY player_name, season
)
SELECT
    COALESCE(cs.player_name, ls.player_name) as player_name,
    COALESCE(ls.first_active_season, cs.current_season) as first_active_season,
    COALESCE(cs.current_season, ls.last_active_season) as last_active_season,
    CASE
        WHEN ls.player_name IS NULL AND cs.player_name IS NOT NULL THEN 'New'
        WHEN ls.last_active_season = cs.current_season - 1 THEN 'Continued Playing'
        WHEN ls.last_active_season < cs.current_season - 1 THEN 'Returned from Retirement'
        WHEN cs.current_season IS NULL AND ls.last_active_season = ls.current_season THEN 'Retired'
        ELSE 'Stayed Retired'

    END as season_state,
    COALESCE(ls.seasons_active,
            ARRAY[]::INT[])
            || CASE WHEN
                cs.player_name IS NOT NULL
                THEN ARRAY[cs.current_season]
                ELSE ARRAY[]::INT[]
                END as seasons_active,
    COALESCE(cs.current_season, ls.current_season + 1) as current_season
FROM current_season cs
FULL OUTER JOIN last_season ls
ON cs.player_name = ls.player_name;