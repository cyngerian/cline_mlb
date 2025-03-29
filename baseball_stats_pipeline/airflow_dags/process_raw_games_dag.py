from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# --- DAG Configuration ---
RAW_JSON_BASE_PATH = "/data/raw_json" # Path inside Airflow worker where volume is mounted

# --- Python Functions for Tasks (Placeholders) ---
def find_completed_games_to_process(**context):
    """
    Placeholder function.
    Scans the RAW_JSON_BASE_PATH for game directories.
    Identifies games marked as 'Final' or completed based on timestamps/status in JSON.
    Returns a list of game_pks or paths to process.
    """
    print(f"Scanning {RAW_JSON_BASE_PATH} for completed games...")
    # In a real implementation:
    # - List directories in RAW_JSON_BASE_PATH (each dir is a game_pk)
    # - For each game_pk, check a marker file or latest JSON status
    # - Return list of game_pks needing processing
    completed_games = ["placeholder_game_pk_1", "placeholder_game_pk_2"] # Example
    print(f"Found games to process: {completed_games}")
    # Push to XCom if needed for downstream tasks
    context['ti'].xcom_push(key='games_to_process', value=completed_games)
    return completed_games

def process_single_game_data(**context):
    """
    Placeholder function.
    Processes the raw JSON files for a single game_pk.
    Performs transformations and loads aggregated data into PostgreSQL.
    """
    # Pull game_pk from context (e.g., if using dynamic task mapping)
    # game_pk = context['params']['game_pk'] # Example if triggered manually with params
    # Or get from upstream task via XCom
    # ti = context['ti']
    # games_list = ti.xcom_pull(task_ids='find_completed_games', key='games_to_process')
    # For now, just print a message
    game_pk = "placeholder_game_pk"
    print(f"Processing raw data for game: {game_pk}")
    # In a real implementation:
    # - Read all JSON files for the game_pk from RAW_JSON_BASE_PATH/{game_pk}
    # - Use pandas or custom logic for transformation/aggregation
    # - Connect to PostgreSQL (using Airflow connections)
    # - Load data into PLAYER_GAME_STATS, TEAM_GAME_STATS, etc.
    print(f"Finished processing game: {game_pk}")


# --- DAG Definition ---
with DAG(
    dag_id="process_raw_game_data",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"), # Adjust start date as needed
    catchup=False,
    schedule="0 6 * * *", # Run daily at 6 AM UTC (adjust as needed)
    tags=["baseball", "etl", "batch"],
    doc_md="""
    ### Process Raw Game Data DAG

    This DAG processes raw JSON game data stored locally.
    - Scans for completed games.
    - Transforms and aggregates data.
    - Loads results into PostgreSQL summary tables.
    """,
) as dag:
    start = EmptyOperator(task_id="start")

    find_completed_games = PythonOperator(
        task_id="find_completed_games",
        python_callable=find_completed_games_to_process,
    )

    # In a real scenario, you might dynamically map 'process_single_game'
    # over the list returned by 'find_completed_games' using Dynamic Task Mapping.
    # For simplicity now, just a single placeholder task.
    process_game_data = PythonOperator(
        task_id="process_game_data",
        python_callable=process_single_game_data,
        # provide_context=True, # Already default in newer Airflow versions
    )

    end = EmptyOperator(task_id="end")

    start >> find_completed_games >> process_game_data >> end
