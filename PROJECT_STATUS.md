# Project Status: Baseball Stats Pipeline

This document tracks the development status of the different components of the pipeline.

---

## 1. Infrastructure (Docker Compose)

*   **Purpose:** Define and manage the core services (Postgres, Kafka, Zookeeper, Airflow) and application containers.
*   **Key Tasks:**
    *   [x] Define base services (Postgres, Kafka, Zookeeper, Airflow) in `docker-compose.yml`
    *   [x] Define application service configurations (`api_poller`, `rt_transformer`, `api_service`)
    *   [ ] Add health checks for critical services
    *   [ ] Configure logging drivers/aggregation (Optional)
*   **Testing Status:**
    *   Tests Written: No (Infrastructure testing often involves integration tests of services using it)
    *   Tests Passing: N/A
    *   Coverage: N/A

---

## 2. API Poller (`api_poller`)

*   **Purpose:** Fetch live game data from the MLB Stats API, save raw JSON to a volume, and publish raw JSON to a Kafka topic.
*   **Key Tasks:**
    *   [x] Basic script structure (`poller.py`)
    *   [x] Fetch schedule to find live games
    *   [x] Fetch live game feed (`/feed/live`)
    *   [x] Save raw JSON to volume
    *   [x] Publish data to Kafka (`live_game_data` topic)
    *   [ ] Implement robust error handling and retries for API calls
    *   [ ] Implement robust error handling for Kafka connection/publishing
    *   [ ] Add configuration options (e.g., poll intervals via env vars) - *Partially done*
    *   [ ] Add comprehensive logging
*   **Testing Status:**
    *   Tests Written: No
    *   Tests Passing: N/A
    *   Coverage: N/A

---

## 3. Real-time Transformer (`rt_transformer`)

*   **Purpose:** Consume raw game data from Kafka, perform basic transformations, and load/update key data points (like game status, score, current play info) into PostgreSQL in near real-time.
*   **Key Tasks:**
    *   [x] Basic script structure (`transformer.py`)
    *   [x] Kafka consumer setup
    *   [x] Database connection setup (SQLAlchemy)
    *   [x] Define basic DB models (`Game`, `GameState`)
    *   [x] Implement basic message processing logic
    *   [x] Implement UPSERT logic for `Game` and `GameState` tables
    *   [ ] Implement robust error handling (Kafka, DB, data parsing)
    *   [ ] Implement manual offset commit strategy - *Partially done*
    *   [ ] Add logic to handle schema evolution/data validation
    *   [ ] Add comprehensive logging
    *   [ ] Populate `Teams` and `Players` tables (or ensure they are populated elsewhere)
*   **Testing Status:**
    *   Tests Written: No
    *   Tests Passing: N/A
    *   Coverage: N/A

---

## 4. Backend API (`api_service`)

*   **Purpose:** Provide a RESTful API (using FastAPI) to query the structured data stored in PostgreSQL.
*   **Key Tasks:**
    *   [x] Basic FastAPI app setup (`main.py`)
    *   [x] Database connection setup (SQLAlchemy)
    *   [x] Define basic Pydantic models for API responses
    *   [x] Implement basic endpoints (`/`, `/games`, `/games/{game_pk}/state`)
    *   [ ] Implement dependency injection for DB sessions - *Done*
    *   [ ] Add more endpoints (e.g., player stats, team stats)
    *   [ ] Implement request validation
    *   [ ] Implement error handling and appropriate HTTP responses
    *   [ ] Add comprehensive logging
    *   [ ] Add API documentation (FastAPI auto-docs help)
*   **Testing Status:**
    *   Tests Written: No
    *   Tests Passing: N/A
    *   Coverage: N/A

---

## 5. Airflow DAGs (`airflow_dags`)

*   **Purpose:** Orchestrate batch processing tasks, primarily for calculating aggregated stats from raw JSON data after games are complete.
*   **Key Tasks:**
    *   [x] Placeholder DAG structure (`process_raw_games_dag.py`)
    *   [ ] Implement logic to find completed games in raw data volume
    *   [ ] Implement robust data transformation logic (e.g., using Pandas) for a single game
    *   [ ] Implement logic to load aggregated stats (e.g., `PLAYER_GAME_STATS`) into PostgreSQL
    *   [ ] Configure Airflow connections (e.g., for PostgreSQL)
    *   [ ] Implement dynamic task mapping (optional, for processing multiple games in parallel)
    *   [ ] Add error handling and alerting
*   **Testing Status:**
    *   Tests Written: No (DAG integrity tests can be written)
    *   Tests Passing: N/A
    *   Coverage: N/A

---

## 6. Testing & CI/CD

*   **Purpose:** Ensure code quality, prevent regressions, and automate checks.
*   **Key Tasks:**
    *   [ ] Set up `pytest` framework for each Python service
    *   [ ] Write unit tests for core logic in each service
    *   [ ] Write integration tests (e.g., testing Kafka publish/consume, DB writes)
    *   [ ] Configure test coverage reporting
    *   [ ] Set up basic GitHub Actions workflow (`.github/workflows/ci.yml`)
    *   [ ] Configure workflow to run linters (`flake8`/`ruff`)
    *   [ ] Configure workflow to run `pytest` and report coverage
*   **Testing Status:** Overall project testing status.

---
*Status Key: [ ] Not Started, [/] In Progress, [x] Completed*
