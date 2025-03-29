# Database Schema Proposal

```mermaid
erDiagram
    GAMES ||--o{ PLAYS : contains
    GAMES ||--o{ GAME_STATE : has_current
    GAMES {
        INT game_pk PK "Unique Game ID from API"
        INT home_team_id FK
        INT away_team_id FK
        DATETIME game_datetime
        VARCHAR venue_name
        VARCHAR game_status "e.g., Preview, Live, Final"
        TIMESTAMP created_at
        TIMESTAMP updated_at
    }

    TEAMS {
        INT team_id PK "Unique Team ID from API"
        VARCHAR name
        VARCHAR abbreviation
        VARCHAR league
        VARCHAR division
        TIMESTAMP created_at
        TIMESTAMP updated_at
    }

    PLAYERS {
        INT player_id PK "Unique Player ID from API"
        VARCHAR full_name
        VARCHAR primary_position
        DATE birth_date
        TIMESTAMP created_at
        TIMESTAMP updated_at
    }

    GAME_STATE ||--|{ GAMES : "tracks_live_state_for"
    GAME_STATE ||--o{ PLAYERS : "current_batter"
    GAME_STATE ||--o{ PLAYERS : "current_pitcher"
    GAME_STATE {
        INT game_pk PK "Primary Key (also FK to GAMES)"
        INT inning
        VARCHAR top_bottom "Top or Bottom"
        INT outs
        INT balls
        INT strikes
        VARCHAR runner_on_first_id FK "Nullable Player ID"
        VARCHAR runner_on_second_id FK "Nullable Player ID"
        VARCHAR runner_on_third_id FK "Nullable Player ID"
        INT current_batter_id FK "Nullable Player ID"
        INT current_pitcher_id FK "Nullable Player ID"
        INT home_score
        INT away_score
        TIMESTAMP last_updated "Timestamp of the last update"
    }

    PLAYS ||--|{ GAMES : "belongs_to"
    PLAYS ||--o{ AT_BATS : "results_in"
    PLAYS {
        VARCHAR play_guid PK "Unique Play GUID from API"
        INT game_pk FK
        INT inning
        VARCHAR top_bottom
        INT play_index "Index within the game"
        VARCHAR description "Text description of the play"
        VARCHAR event_type "e.g., Strikeout, Single, HomeRun"
        TIMESTAMP created_at
    }

    AT_BATS ||--|{ PLAYS : "part_of"
    AT_BATS ||--o{ PLAYERS : "by_batter"
    AT_BATS ||--o{ PLAYERS : "against_pitcher"
    AT_BATS ||--o{ PITCHES : contains
    AT_BATS {
        VARCHAR at_bat_guid PK "Derived or from API"
        VARCHAR play_guid FK
        INT game_pk FK
        INT batter_id FK
        INT pitcher_id FK
        VARCHAR result_event "e.g., Single, Strikeout"
        TIMESTAMP start_time
        TIMESTAMP end_time
        TIMESTAMP created_at
    }

    PITCHES ||--|{ AT_BATS : "part_of"
    PITCHES {
        VARCHAR pitch_guid PK "Derived or from API"
        VARCHAR at_bat_guid FK
        INT pitch_index "Index within the at-bat"
        VARCHAR pitch_type "e.g., FF (Fastball), CU (Curveball)"
        FLOAT start_speed
        FLOAT end_speed
        VARCHAR call "e.g., Strike, Ball, InPlay"
        BOOLEAN is_strike
        BOOLEAN is_ball
        TIMESTAMP pitch_time
        TIMESTAMP created_at
    }

    GAMES }o--|| TEAMS : "home_team"
    GAMES }o--|| TEAMS : "away_team"

    PLAYER_GAME_STATS {
        INT game_pk PK "Part of Composite PK (also FK to GAMES)"
        INT player_id PK "Part of Composite PK (also FK to PLAYERS)"
        INT ab
        INT r
        INT h
        INT rbi
        INT bb
        INT so
        FLOAT ip
        INT h_allowed
        INT r_allowed
        INT er
        INT bb_allowed
        INT so_pitched
    }

    TEAM_GAME_STATS {
        INT game_pk PK "Part of Composite PK (also FK to GAMES)"
        INT team_id PK "Part of Composite PK (also FK to TEAMS)"
        INT runs_scored
        INT hits
        INT errors
    }

    PLAYER_GAME_STATS ||--|{ GAMES : "stats_for_game"
    PLAYER_GAME_STATS ||--|{ PLAYERS : "stats_for_player"
    TEAM_GAME_STATS ||--|{ GAMES : "stats_for_game"
    TEAM_GAME_STATS ||--|{ TEAMS : "stats_for_team"
