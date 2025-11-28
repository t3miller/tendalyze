-- schema.sql
-- Clean schema that matches web/app.py and the ETL.

-- 1) TEAMS
CREATE TABLE IF NOT EXISTS teams (
    team_id     SERIAL PRIMARY KEY,
    team_name   TEXT NOT NULL,
    mascot      TEXT,
    city        TEXT,
    state       TEXT,
    division    TEXT,
    region      TEXT,
    district    TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- basic dedupe protection
    CONSTRAINT teams_unique_name_location
        UNIQUE (team_name, COALESCE(city, ''), COALESCE(state, ''))
);

-- 2) GAMES
CREATE TABLE IF NOT EXISTS games (
    game_id          SERIAL PRIMARY KEY,
    offense_team_id  INT NOT NULL REFERENCES teams(team_id),
    defense_team_id  INT NOT NULL REFERENCES teams(team_id),
    game_date        DATE,
    season           INT,
    week             INT,
    venue            TEXT,
    source           TEXT DEFAULT 'hudl',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 3) DRIVES
CREATE TABLE IF NOT EXISTS drives (
    drive_id         SERIAL PRIMARY KEY,
    game_id          INT NOT NULL REFERENCES games(game_id),
    offense_team_id  INT NOT NULL REFERENCES teams(team_id),
    defense_team_id  INT NOT NULL REFERENCES teams(team_id),

    start_quarter    INT,
    start_clock      TEXT,
    start_yardline   INT,
    end_yardline     INT,
    result           TEXT
);

-- 4) PLAYS
CREATE TABLE IF NOT EXISTS plays (
    play_id          SERIAL PRIMARY KEY,
    drive_id         INT REFERENCES drives(drive_id),
    game_id          INT NOT NULL REFERENCES games(game_id),

    offense_team_id  INT REFERENCES teams(team_id),
    defense_team_id  INT REFERENCES teams(team_id),

    quarter          INT,
    clock            TEXT,
    down             INT,
    distance         INT,
    yard_line        INT,
    hash_mark        TEXT,

    formation_raw    TEXT,
    formation_norm   TEXT,
    personnel        TEXT,
    play_type        TEXT,
    pass_zone        TEXT,
    run_direction    TEXT,

    yards_gained     INT,
    result           TEXT
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_games_season_week
    ON games (season, week);

CREATE INDEX IF NOT EXISTS idx_plays_game_id
    ON plays (game_id);

CREATE INDEX IF NOT EXISTS idx_plays_formation_norm
    ON plays (formation_norm);
