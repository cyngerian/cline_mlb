# Use Case: Home Run Tracker Web App

This document outlines the plan for developing the first major use case for the Baseball Stats Pipeline: a web application focused on tracking and analyzing home runs.

## 1. Use Case Description

The goal is to create a web application that allows users to:

*   View data related to home runs from multiple perspectives: batter, pitcher, matchup, and park.
*   See calculated statistics like HR/9 innings pitched (pitchers) and HR/plate appearance (batters).
*   Explore historical matchup data between specific batters and pitchers regarding home runs.
*   View information about home run frequency in different ballparks.
*   Track games live, specifically focusing on home runs hit during the current day.
*   See advanced details for home runs, such as hit trajectory (launch angle) and exit velocity (launch speed).
*   Receive near real-time updates, such as notifications or indicators when selected batters are due up soon (e.g., on deck, in the hole).
*   (Future) Join groups, select batters for tracking, input betting odds, and follow potential home runs within the group context.

The web application will primarily interact with the `api_service` component to retrieve the necessary data.

## 2. Implementation Phases

This use case will be implemented in phases to manage complexity and deliver value incrementally.

### Phase 1: Core Play Data & API

*   **Goal:** Establish the foundation by capturing detailed play-by-play data, specifically identifying home runs and their attributes, and exposing a basic API endpoint for today's home runs. Demonstrate the data flow from raw storage (MinIO) through processing to the API.
*   **Key Tasks:**
    *   **Schema Design:**
        *   [ ] Define schema for a new `plays` table in PostgreSQL. Key columns: `play_id` (unique ID for the play within the game), `game_pk` (FK to `games`), `inning`, `top_bottom`, `at_bat_index`, `batter_id` (FK to `players`), `pitcher_id` (FK to `players`), `result_type`, `result_description`, `event_type` (e.g., 'home_run'), `rbi`, `hit_data_launch_speed`, `hit_data_launch_angle`, `hit_data_total_distance`, `play_start_time`, `play_end_time`.
        *   [ ] Define schema for `players` table (if not already sufficient). Ensure `player_id` is primary key.
        *   [ ] Define schema for `teams` table (if not already sufficient). Ensure `team_id` is primary key.
        *   [ ] Define schema for `venues` table. Include `venue_id`, `name`, `city`, `state`, potentially park factor columns later.
    *   **Data Processing (Decision: Airflow DAG):**
        *   [/] Update `airflow_dags/process_raw_games_dag.py` (or create a new DAG).
        *   [ ] Implement Airflow task to read raw JSON game data from MinIO (using S3 hook/operator). Target completed games first.
        *   [ ] Implement Python logic (e.g., within a `PythonOperator`) to parse the `allPlays` array from the raw JSON.
        *   [ ] Implement logic to extract relevant fields for the `plays` table, focusing on identifying home runs and their hit data.
        *   [ ] Implement logic to populate the `plays` table in PostgreSQL (using Postgres hook/operator). Handle potential duplicates/updates if DAG reruns.
        *   [ ] Implement logic (potentially in a separate DAG or task) to populate `players`, `teams`, and `venues` tables from game data if they are empty.
    *   **API Enhancement (`api_service`):**
        *   [ ] Define Pydantic models for the `Play` data.
        *   [ ] Create a new API endpoint (e.g., `/homeruns/today`) that queries the `plays` table for events of type 'home_run' within the current date.
        *   [ ] Include relevant details like batter, pitcher, inning, distance, speed, angle in the API response.
    *   **Testing:**
        *   [ ] Write unit tests for the Airflow DAG parsing logic.
        *   [ ] Write integration tests for the new API endpoint.
        *   [ ] Perform manual E2E test: Run backfill, trigger DAG, query API endpoint.

### Phase 2: Calculated Stats & API

*   **Goal:** Calculate and expose key aggregated statistics related to home runs (HR/PA, HR/9).
*   **Key Tasks:**
    *   **Schema Design:**
        *   [ ] Define schema for `player_game_stats` table (e.g., `player_id`, `game_pk`, `PA`, `HR`, `SO`, etc.).
        *   [ ] Define schema for `pitcher_game_stats` table (e.g., `pitcher_id`, `game_pk`, `IP`, `BF`, `HR_allowed`, `SO`, etc.).
        *   [ ] Define schema for `player_season_stats` table (e.g., `player_id`, `season`, `PA`, `HR`, `HR_per_PA`, etc.).
        *   [ ] Define schema for `pitcher_season_stats` table (e.g., `pitcher_id`, `season`, `IP`, `BF`, `HR_allowed`, `HR_per_9`, etc.).
    *   **Data Processing (Airflow DAGs):**
        *   [ ] Create/Update Airflow DAG(s) to run periodically (e.g., daily after games complete).
        *   [ ] Implement task(s) to calculate game-level stats (`player_game_stats`, `pitcher_game_stats`) by aggregating data from the `plays` table.
        *   [ ] Implement task(s) to calculate season-level stats (`player_season_stats`, `pitcher_season_stats`) by aggregating game-level stats. Include HR/PA and HR/9 calculations.
    *   **API Enhancement (`api_service`):**
        *   [ ] Define Pydantic models for the aggregated stats.
        *   [ ] Create new API endpoints to query player and pitcher season stats (e.g., `/stats/player/season/{season}`, `/stats/pitcher/season/{season}`). Allow filtering/sorting.
    *   **Testing:**
        *   [ ] Write unit tests for the stats calculation logic in Airflow DAGs.
        *   [ ] Write integration tests for the new API endpoints.

### Phase 3: Advanced Features & UI

*   **Goal:** Implement live tracking features, matchup/park data, user interaction, and potentially a basic web UI.
*   **Key Tasks:**
    *   **Live Tracking (`rt_transformer` / `api_service`):**
        *   [ ] Enhance `rt_transformer` to parse lineup/due-up information from the live feed if available.
        *   [ ] Update `game_state` table or create a new table for current/upcoming batters.
        *   [ ] Enhance `/games/{game_pk}/state` endpoint in `api_service` to include upcoming batter info.
    *   **Matchup/Park Data (Airflow / `api_service`):**
        *   [ ] Define schema for `batter_pitcher_matchups` table (e.g., `batter_id`, `pitcher_id`, `PA`, `HR`).
        *   [ ] Implement Airflow task to calculate matchup history from the `plays` table.
        *   [ ] Implement logic to calculate basic park factors (requires more historical data).
        *   [ ] Create API endpoints for matchup history and park data.
    *   **User Features (New Service / `api_service`):**
        *   [ ] Design schema for users, groups, picks, odds.
        *   [ ] Implement user authentication.
        *   [ ] Create API endpoints for managing groups, picks, odds.
        *   *(Note: This might warrant a separate microservice)*
    *   **Web Application (UI):**
        *   [ ] Choose a frontend framework (React, Vue, Svelte, etc.).
        *   [ ] Develop UI components to display data from the API endpoints.
        *   [ ] Implement live polling from the frontend to the `api_service`.
    *   **Testing:**
        *   [ ] Write tests for new processing logic and API endpoints.
        *   [ ] Write E2E tests for the web application.

*(Status Key: [ ] Not Started, [/] In Progress, [x] Completed)*
