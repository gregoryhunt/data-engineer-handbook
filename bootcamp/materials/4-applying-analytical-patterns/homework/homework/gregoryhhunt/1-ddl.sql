CREATE TABLE players_state_tracking (
    player_name TEXT,
    first_active_season INT,
    last_active_season INT,
    season_state TEXT,
    seasons_active INT[],
    current_season INT,
    PRIMARY KEY (player_name, current_season)
);