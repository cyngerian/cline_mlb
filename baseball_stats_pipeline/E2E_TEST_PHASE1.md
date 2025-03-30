# End-to-End Test Plan: Phase 1 (Home Run Tracker)

This document outlines the steps to test the core data flow for Phase 1 of the Home Run Tracker use case, verifying that play-by-play data (especially home runs) is processed from MinIO via Airflow, stored in PostgreSQL, and accessible via the API service.

**Prerequisites:**

*   Docker environment is running.
*   The main pipeline services have been started at least once (`docker-compose -f baseball_stats_pipeline/docker-compose.yml up -d`).
*   Raw game data exists in the MinIO `baseball-stats-raw` bucket (e.g., from running the `api_poller` backfill).

**Manual Setup Steps (Airflow):**

1.  **Install Airflow Providers:** Ensure the necessary providers are installed in your Airflow environment. If using the provided `docker-compose.yml` with the `puckel/docker-airflow` image, you might need to add these packages, potentially by modifying the `docker-compose.yml` for the `airflow-webserver` and `airflow-scheduler` services or building a custom image. The required providers are:
    *   `apache-airflow-providers-postgres`
    *   `apache-airflow-providers-amazon` (for S3/MinIO interaction)
    *   *Restart Airflow containers after installation if necessary.*
2.  **Configure Airflow Connections:**
    *   Access the Airflow UI (usually `http://localhost:8080`).
    *   Go to `Admin` -> `Connections`.
    *   **Add/Edit `postgres_default` Connection:**
        *   Conn Id: `postgres_default`
        *   Conn Type: `Postgres`
        *   Host: `postgres` (the service name in `docker-compose.yml`)
        *   Schema: `baseball_stats`
        *   Login: `statsuser`
        *   Password: `statspassword`
        *   Port: `5432`
    *   **Add `minio_s3` Connection:**
        *   Conn Id: `minio_s3`
        *   Conn Type: `Amazon S3`
        *   AWS Access Key ID: `minioadmin` (default from `docker-compose.yml`)
        *   AWS Secret Access Key: `minioadmin` (default from `docker-compose.yml`)
        *   Extra: `{"endpoint_url": "http://minio:9000"}` (Specify the MinIO endpoint URL within the Docker network)

**Testing Steps:**

1.  **Restart Services:** Ensure the latest code changes are picked up and the new tables are created.
    *   **Command:**
        ```bash
        docker-compose -f baseball_stats_pipeline/docker-compose.yml restart rt_transformer api_service airflow-scheduler airflow-webserver
        ```
    *   **Expected Outcome:** Containers restart successfully. Check logs for `rt_transformer` to confirm it connects to the DB and potentially mentions table creation/checking.

2.  **Trigger Airflow DAG:**
    *   **Action:** Go to the Airflow UI (`http://localhost:8080`). Find the `process_game_plays_from_s3` DAG. Unpause it if it's paused. Trigger it manually using the "Trigger DAG w/ config" option (play button with a wrench).
    *   **Configuration (Optional):** You can override the default `process_date` if needed (e.g., `"process_date": "2025-03-29"`). Otherwise, it will default to yesterday.
    *   **Expected Outcome:** A new DAG run appears and starts executing.

3.  **Monitor DAG Run:**
    *   **Action:** Click on the running DAG instance in the Airflow UI. Monitor the tasks (`list_completed_games_for_date`, `process_single_game`) in the Graph or Gantt view. Check the logs for each task instance.
    *   **Expected Outcome:**
        *   `list_completed_games_for_date` should successfully list game PKs found in MinIO under a `/final/` path structure (Note: the current listing logic might need refinement based on actual S3 paths).
        *   `process_single_game` tasks (one for each game found) should:
            *   Log reading files from S3.
            *   Log extracting plays.
            *   Log upserting plays to PostgreSQL.
        *   The overall DAG run should eventually succeed.

4.  **Verify API Endpoint:**
    *   **Action:** Once the DAG run has successfully completed, query the new API endpoint.
    *   **Command:**
        ```bash
        docker-compose -f baseball_stats_pipeline/docker-compose.yml exec api_poller python -c "import urllib.request, json; print(json.dumps(json.loads(urllib.request.urlopen('http://api_service:8000/homeruns?limit=10').read().decode('utf-8')), indent=2))"
        ```
        *(This command fetches the first 10 home runs and pretty-prints the JSON)*
    *   **Expected Outcome:** The command should output a JSON array containing details for home runs processed by the Airflow DAG. Each object in the array should match the `HomeRunDetail` Pydantic model, including `game_pk`, `inning`, `batter` (with id/name), `pitcher` (with id/name), `rbi`, and hit data (`hit_data_launch_speed`, etc.) if available in the source data for that play. If no home runs were hit in the processed games, an empty array `[]` is expected.

5.  **(Optional) Verify Database:**
    *   **Action:** Connect to the PostgreSQL database directly (using DBeaver, `psql`, etc.) with the credentials mentioned in the setup.
    *   **Command (Example):**
        ```sql
        SELECT p.game_pk, p.inning, p.result_description, p.hit_data_launch_speed, b.full_name as batter, pt.full_name as pitcher
        FROM plays p
        LEFT JOIN players b ON p.batter_id = b.player_id
        LEFT JOIN players pt ON p.pitcher_id = pt.player_id
        WHERE p.result_event = 'Home Run'
        ORDER BY p.play_end_time DESC NULLS LAST
        LIMIT 10;
        ```
    *   **Expected Outcome:** The query should return rows corresponding to the home runs, matching the data seen via the API.

This sequence tests the batch processing path from MinIO through Airflow to PostgreSQL and verifies accessibility via the API service.
