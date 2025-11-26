CREATE TABLE IF NOT EXISTS teams (
    team_id SERIAL PRIMARY KEY,
    team_name TEXT NOT NULL,
    team_code TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS games (
    game_id SERIAL PRIMARY KEY,
    season INT NOT NULL,
    week INT,
    game_date DATE,
    home_team_id INT REFERENCES teams(team_id),
    away_team_id INT REFERENCES teams(team_id),
    home_score INT,
    away_score INT
);

CREATE TABLE IF NOT EXISTS drives (
    drive_id SERIAL PRIMARY KEY,
    game_id INT REFERENCES games(game_id),
    offense_team_id INT REFERENCES teams(team_id),
    defense_team_id INT REFERENCES teams(team_id),
    start_quarter INT,
    start_clock TEXT,
    start_yardline INT,
    end_yardline INT,
    result TEXT
);

CREATE TABLE IF NOT EXISTS plays (
    play_id SERIAL PRIMARY KEY,
    drive_id INT REFERENCES drives(drive_id),
    game_id INT REFERENCES games(game_id),

    offense_team_id INT REFERENCES teams(team_id),
    defense_team_id INT REFERENCES teams(team_id),

    quarter INT,
    clock TEXT,
    down INT,
    distance INT,
    yard_line INT,
    hash_mark TEXT,

    formation_raw TEXT,
    formation_norm TEXT,
    personnel TEXT,
    play_type TEXT,
    run_direction TEXT,
    pass_zone TEXT,

    yards_gained INT,
    result TEXT
);
