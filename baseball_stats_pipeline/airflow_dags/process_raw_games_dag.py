from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone

import pendulum

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.empty import EmptyOperator
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import Table, MetaData, Column, Integer, String, DateTime, Float, Boolean, Text, PrimaryKeyConstraint

# --- Constants ---
S3_CONN_ID = "minio_s3" # Airflow Connection ID for MinIO/S3
S3_BUCKET = "baseball-stats-raw" # Bucket name where raw data is stored
POSTGRES_CONN_ID = "postgres_default" # Airflow Connection ID for PostgreSQL

log = logging.getLogger(__name__)

# Define table metadata for SQLAlchemy UPSERT helper
# (Ideally, this would be shared with rt_transformer models)
metadata = MetaData()
plays_table = Table(
    'plays', metadata,
    Column('game_pk', Integer, primary_key=True),
    Column('at_bat_index', Integer, primary_key=True),
    Column('play_event_index', Integer, primary_key=True),
    Column('inning', Integer, nullable=False),
    Column('top_bottom', String(10), nullable=False),
    Column('batter_id', Integer), # FK handled by DB schema
    Column('pitcher_id', Integer), # FK handled by DB schema
    Column('result_type', String(50)),
    Column('result_event', String(50)),
    Column('result_description', Text),
    Column('rbi', Integer),
    Column('is_scoring_play', Boolean),
    Column('hit_data_launch_speed', Float, nullable=True),
    Column('hit_data_launch_angle', Float, nullable=True),
    Column('hit_data_total_distance', Float, nullable=True),
    Column('play_start_time', DateTime(timezone=True), nullable=True),
    Column('play_end_time', DateTime(timezone=True), nullable=True),
    Column('created_at', DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)),
    PrimaryKeyConstraint('game_pk', 'at_bat_index', 'play_event_index')
)

# Helper function to parse datetime strings safely
def parse_datetime(dt_string):
    if not dt_string:
        return None
    try:
        # Handle potential 'Z' for UTC
        if dt_string.endswith('Z'):
            dt_string = dt_string[:-1] + '+00:00'
        return datetime.fromisoformat(dt_string)
    except (ValueError, TypeError):
        log.warning(f"Could not parse datetime string: {dt_string}")
        return None

@dag(
    dag_id="process_game_plays_from_s3",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    schedule=timedelta(hours=1), # Run hourly, or "0 6 * * *" for daily at 6 AM UTC
    tags=["baseball", "etl", "s3", "postgres", "plays"],
    doc_md="""
    ### Process Game Plays DAG

    This DAG processes raw JSON game data stored in S3 (MinIO).
    - Lists completed games (status='final') in the S3 bucket.
    - For each game, reads all JSON snapshots.
    - Extracts play-by-play data.
    - Upserts play data into the `plays` table in PostgreSQL.
    """,
    params={
        "s3_bucket": Param(S3_BUCKET, type="string", title="S3 Bucket"),
        "postgres_conn_id": Param(POSTGRES_CONN_ID, type="string", title="Postgres Conn Id"),
        "s3_conn_id": Param(S3_CONN_ID, type="string", title="S3 Conn Id"),
        "process_date": Param(
            (pendulum.now("UTC") - timedelta(days=1)).to_date_string(),
            type="string",
            title="Process Date (YYYY-MM-DD)",
            description="Process games that finished on this date. Defaults to yesterday.",
        )
    }
)
def process_game_plays_dag():

    @task
    def list_completed_games_for_date(process_date: str, s3_conn_id: str, bucket: str) -> list[str]:
        """
        Lists game_pk values for games marked as 'final' within a specific date's S3 path structure.
        Assumes path structure like: <season>/<game_type>/final/<game_pk>/...
        """
        s3_hook = S3Hook(aws_conn_id=s3_conn_id)
        game_pks = set()
        # We need to infer season and game_type or list more broadly.
        # Let's assume we process for a specific date, regardless of season/type for now.
        # A more robust approach might list seasons/types first.
        # Example prefix for a specific date (needs adjustment based on actual data layout):
        # This is tricky because the date isn't directly in the path structure defined earlier.
        # Let's assume we list all 'final' games for simplicity now.
        # A better approach might be to query the 'games' table in Postgres first.

        log.info(f"Listing 'final' games in bucket '{bucket}'...")
        # Listing directories under any path ending in '/final/'
        # This might be inefficient on large buckets.
        prefixes = s3_hook.list_prefixes(bucket_name=bucket, delimiter='/')
        final_game_prefixes = []

        # This logic needs refinement based on actual S3 layout and desired scope.
        # Simplified: Find prefixes ending in /final/
        possible_final_paths = []
        if prefixes:
             for prefix in prefixes: # e.g., 2024/
                 season_prefixes = s3_hook.list_prefixes(bucket_name=bucket, prefix=prefix, delimiter='/')
                 if season_prefixes:
                     for sp in season_prefixes: # e.g., 2024/regular/
                         status_prefixes = s3_hook.list_prefixes(bucket_name=bucket, prefix=sp, delimiter='/')
                         if status_prefixes:
                             for stp in status_prefixes: # e.g., 2024/regular/final/
                                 if stp.endswith('/final/'):
                                     possible_final_paths.append(stp)

        log.info(f"Found potential final game paths: {possible_final_paths}")

        for path in possible_final_paths:
            game_pk_prefixes = s3_hook.list_prefixes(bucket_name=bucket, prefix=path, delimiter='/')
            if game_pk_prefixes:
                for gp in game_pk_prefixes:
                    # Extract game_pk (assuming format .../final/GAME_PK/)
                    try:
                        game_pk = gp.split('/')[-2]
                        if game_pk.isdigit():
                            game_pks.add(game_pk)
                    except IndexError:
                        log.warning(f"Could not extract game_pk from prefix: {gp}")

        log.info(f"Found {len(game_pks)} completed games to process: {list(game_pks)}")
        # TODO: Add logic to check if game_pk was already processed successfully for this date.
        return list(game_pks)

    @task
    def process_single_game(game_pk: str, s3_conn_id: str, bucket: str, postgres_conn_id: str):
        """
        Reads all JSON files for a game_pk from S3, extracts play data, and upserts to Postgres.
        """
        log.info(f"Processing game_pk: {game_pk}")
        s3_hook = S3Hook(aws_conn_id=s3_conn_id)
        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        all_plays_data = []

        # Find the S3 prefix for this game_pk under 'final' status
        # This assumes the game IS final. Needs refinement based on list_completed_games logic.
        # Example: Find keys under any path like '.../final/GAME_PK/'
        game_keys = s3_hook.list_keys(bucket_name=bucket, prefix=f"final/{game_pk}/", delimiter='/') # Simplified prefix
        # A more robust search might be needed if season/type vary
        # game_keys = s3_hook.list_keys(bucket_name=bucket, prefix=f"{season}/{game_type}/final/{game_pk}/")

        if not game_keys:
             # Try searching without status if listing logic changes
             log.warning(f"No keys found under 'final/{game_pk}/'. Trying without status...")
             game_keys = s3_hook.list_keys(bucket_name=bucket, prefix=f"{game_pk}/", delimiter='/') # Even more simplified

        if not game_keys:
            log.warning(f"No S3 keys found for game_pk {game_pk}. Skipping.")
            return

        log.info(f"Found {len(game_keys)} S3 files for game_pk {game_pk}.")

        # Read all JSON files for the game
        game_data_snapshots = []
        for key in game_keys:
            try:
                content = s3_hook.read_key(key=key, bucket_name=bucket)
                game_data_snapshots.append(json.loads(content))
            except Exception as e:
                log.error(f"Error reading or parsing S3 key {key}: {e}")
                continue # Skip this file

        if not game_data_snapshots:
            log.warning(f"No valid JSON data found for game_pk {game_pk}. Skipping.")
            return

        # Process plays from the *last* snapshot (assuming it has the complete game)
        # A more robust approach might merge plays if needed, but this is simpler.
        last_snapshot = game_data_snapshots[-1] # Assumes sorted by timestamp implicitly
        plays = last_snapshot.get("liveData", {}).get("plays", {}).get("allPlays", [])

        log.info(f"Extracted {len(plays)} plays from the last snapshot for game_pk {game_pk}.")

        for play in plays:
            about = play.get("about", {})
            result = play.get("result", {})
            matchup = play.get("matchup", {})
            hit_data = play.get("hitData", {})
            play_events = play.get("playEvents", [])
            last_event_index = play_events[-1].get("index") if play_events else -1 # Use -1 if no events

            play_record = {
                "game_pk": int(game_pk),
                "at_bat_index": about.get("atBatIndex"),
                "play_event_index": last_event_index,
                "inning": about.get("inning"),
                "top_bottom": about.get("halfInning", "").capitalize(),
                "batter_id": matchup.get("batter", {}).get("id"),
                "pitcher_id": matchup.get("pitcher", {}).get("id"),
                "result_type": result.get("type"),
                "result_event": result.get("event"),
                "result_description": result.get("description"),
                "rbi": result.get("rbi"),
                "is_scoring_play": about.get("isScoringPlay"),
                "hit_data_launch_speed": hit_data.get("launchSpeed"),
                "hit_data_launch_angle": hit_data.get("launchAngle"),
                "hit_data_total_distance": hit_data.get("totalDistance"),
                "play_start_time": parse_datetime(about.get("startTime")),
                "play_end_time": parse_datetime(about.get("endTime")),
                "created_at": datetime.now(timezone.utc)
            }
            # Filter out records with missing composite key parts
            if play_record["at_bat_index"] is not None and play_record["play_event_index"] is not None:
                 all_plays_data.append(play_record)
            else:
                 log.warning(f"Skipping play due to missing index: game={game_pk}, about={about}")


        if not all_plays_data:
            log.info(f"No valid play records extracted for game_pk {game_pk}.")
            return

        log.info(f"Upserting {len(all_plays_data)} play records for game_pk {game_pk}...")

        # Perform bulk upsert
        try:
            # Define columns to update on conflict
            update_columns = [
                col.name for col in plays_table.c
                if col.name not in ['game_pk', 'at_bat_index', 'play_event_index', 'created_at']
            ]
            # Create the UPSERT statement using SQLAlchemy helper
            stmt = pg_insert(plays_table).values(all_plays_data)
            stmt = stmt.on_conflict_do_update(
                index_elements=['game_pk', 'at_bat_index', 'play_event_index'],
                set_={col: getattr(stmt.excluded, col) for col in update_columns}
            )
            # Execute using PostgresHook
            pg_hook.run(str(stmt.compile(compile_kwargs={"literal_binds": True})), autocommit=True)
            log.info(f"Successfully upserted plays for game_pk {game_pk}.")
        except Exception as e:
            log.error(f"Error upserting plays for game_pk {game_pk}: {e}")
            # Consider adding retry logic or dead-letter queue for failures

    # --- DAG Structure ---
    start = EmptyOperator(task_id="start")

    # Task to find completed games
    # Note: This implementation needs refinement based on S3 structure / alternative methods
    list_games = list_completed_games_for_date(
        process_date="{{ params.process_date }}",
        s3_conn_id="{{ params.s3_conn_id }}",
        bucket="{{ params.s3_bucket }}"
    )

    # Dynamically map the processing task over the list of games
    process_games = process_single_game.partial(
        s3_conn_id="{{ params.s3_conn_id }}",
        bucket="{{ params.s3_bucket }}",
        postgres_conn_id="{{ params.postgres_conn_id }}"
    ).expand(game_pk=list_games)

    end = EmptyOperator(task_id="end")

    start >> list_games >> process_games >> end

process_game_plays_dag()
