with game_data AS (
SELECT gd.player_name,
      g.game_date_est,
      CASE
          WHEN pts > 10 THEN 1
          ELSE 0
          END as over_10_pts
FROM game_details gd
        LEFT JOIN games g
                  ON gd.game_id = g.game_id
WHERE player_name = 'LeBron James'
ORDER BY 2
),
streak_calc AS (
   SELECT
       player_name,
       game_date_est,
       over_10_pts,
       SUM(CASE WHEN over_10_pts = 0 THEN 1 ELSE 0 END) OVER (ORDER BY game_date_est) as streak_group
   FROM game_data
),
streaks AS (
   SELECT
       player_name,
       streak_group,
       COUNT(*) as streak_length
   FROM streak_calc
   WHERE over_10_pts = 1
   GROUP BY player_name, streak_group
)
SELECT player_name, MAX(streak_length) as longest_streak
FROM streaks
GROUP BY player_name;