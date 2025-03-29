import pytest
from unittest.mock import MagicMock, patch, call, ANY
import json
from datetime import datetime
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.sql.dml import Insert # To check instance type

# Import the functions/classes to test from the transformer script
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from transformer import process_message, Game, GameState # Import models too

# --- Test Data ---
MOCK_KAFKA_MESSAGE = MagicMock()
MOCK_GAME_DATA = {
    "gamePk": 745804,
    "gameData": {
        "teams": {
            "home": {"id": 111, "name": "Boston Red Sox"},
            "away": {"id": 147, "name": "New York Yankees"}
        },
        "datetime": {"dateTime": "2024-03-29T23:05:00Z"},
        "status": {"abstractGameState": "Live"},
        "venue": {"name": "Fenway Park"}
    },
    "liveData": {
        "linescore": {
            "currentInning": 3,
            "isTopInning": False, # Bottom of the inning
            "teams": {"home": {"runs": 2}, "away": {"runs": 1}}
        },
        "plays": {
            "currentPlay": {
                "about": {"outs": 1},
                "matchup": {
                    "batter": {"id": 666, "fullName": "Batter Up"},
                    "pitcher": {"id": 777, "fullName": "Pitcher Perfect"},
                    "count": {"balls": 2, "strikes": 1}
                },
                "runners": [
                    {"movement": {"end": "1B"}, "details": {"runner": {"id": 123}}},
                    {"movement": {"end": "3B"}, "details": {"runner": {"id": 456}}}
                ]
            }
        }
    },
    "metaData": {"timeStamp": "20240329_231500"}
}
MOCK_KAFKA_MESSAGE.value = MOCK_GAME_DATA
MOCK_KAFKA_MESSAGE.offset = 101

# --- Tests for process_message ---

@patch('transformer.pg_insert') # Mock the postgresql insert function
def test_process_message_success(mock_pg_insert):
    """Test successful processing of a Kafka message."""
    mock_db_session = MagicMock()
    # Mock the return value of pg_insert to allow chaining .on_conflict_do_update
    mock_insert_stmt = MagicMock()
    mock_pg_insert.return_value = mock_insert_stmt
    mock_upsert_stmt = MagicMock()
    mock_insert_stmt.on_conflict_do_update.return_value = mock_upsert_stmt

    process_message(MOCK_KAFKA_MESSAGE, mock_db_session)

    # --- Assertions for Game Upsert ---
    # Check pg_insert was called for Game table
    game_call = call(Game)
    assert game_call in mock_pg_insert.call_args_list
    # Find the specific call and check its values
    game_insert_call = next(c for c in mock_pg_insert.call_args_list if c[0][0] == Game)
    expected_game_values = {
        "game_pk": 745804,
        "home_team_id": 111,
        "away_team_id": 147,
        "game_datetime": datetime(2024, 3, 29, 23, 5, 0), # Parsed datetime
        "venue_name": "Fenway Park",
        "game_status": "Live",
        "updated_at": ANY # Check that it's present, value is tricky
    }
    # Check that .values() was called with the correct data
    assert mock_insert_stmt.values.call_args[0][0] == expected_game_values

    # Check on_conflict_do_update was called correctly for Game
    mock_insert_stmt.on_conflict_do_update.assert_called_once()
    assert mock_insert_stmt.on_conflict_do_update.call_args[1]['index_elements'] == ['game_pk']
    # Check that the 'set_' argument contains the right keys (excluding pk)
    assert set(mock_insert_stmt.on_conflict_do_update.call_args[1]['set_'].keys()) == set(expected_game_values.keys()) - {'game_pk'}

    # --- Assertions for GameState Upsert ---
    # Check pg_insert was called for GameState table
    gs_call = call(GameState)
    assert gs_call in mock_pg_insert.call_args_list
    # Find the specific call and check its values
    gs_insert_call = next(c for c in mock_pg_insert.call_args_list if c[0][0] == GameState)
    expected_gs_values = {
        "game_pk": 745804,
        "inning": 3,
        "top_bottom": "Bottom",
        "outs": 1,
        "balls": 2,
        "strikes": 1,
        "runner_on_first_id": 123,
        "runner_on_second_id": None,
        "runner_on_third_id": 456,
        "current_batter_id": 666,
        "current_pitcher_id": 777,
        "home_score": 2,
        "away_score": 1,
        "last_updated": ANY
    }
    # Check that .values() was called with the correct data
    assert mock_insert_stmt.values.call_args[0][0] == expected_gs_values

    # Check on_conflict_do_update was called correctly for GameState
    # Note: This reuses mock_insert_stmt, need to check second call or separate mocks
    # For simplicity, let's assume the second call to on_conflict_do_update is for GameState
    assert mock_insert_stmt.on_conflict_do_update.call_count == 2
    last_on_conflict_call = mock_insert_stmt.on_conflict_do_update.call_args_list[-1]
    assert last_on_conflict_call[1]['index_elements'] == ['game_pk']
    assert set(last_on_conflict_call[1]['set_'].keys()) == set(expected_gs_values.keys()) - {'game_pk'}


    # Check db_session.execute was called twice (once for each upsert)
    assert mock_db_session.execute.call_count == 2
    # Check commit was called
    mock_db_session.commit.assert_called_once()
    # Check rollback was not called
    mock_db_session.rollback.assert_not_called()

@patch('transformer.pg_insert')
@patch('transformer.logger') # Mock logger to check warnings/errors
def test_process_message_incomplete_data(mock_logger, mock_pg_insert):
    """Test processing a message with missing essential data."""
    mock_db_session = MagicMock()
    incomplete_message = MagicMock()
    incomplete_message.value = {"gamePk": 123} # Missing gameData and liveData
    incomplete_message.offset = 102

    process_message(incomplete_message, mock_db_session)

    # Assert that no database operations happened
    mock_pg_insert.assert_not_called()
    mock_db_session.execute.assert_not_called()
    mock_db_session.commit.assert_not_called()
    mock_db_session.rollback.assert_not_called()
    # Assert warning was logged
    mock_logger.warning.assert_called_once_with(
        f"Incomplete data received for message offset {incomplete_message.offset}. Skipping."
    )

@patch('transformer.pg_insert', side_effect=Exception("DB Error")) # Simulate DB error
@patch('transformer.logger')
def test_process_message_db_error(mock_logger, mock_pg_insert):
    """Test handling of database errors during processing."""
    mock_db_session = MagicMock()

    process_message(MOCK_KAFKA_MESSAGE, mock_db_session)

    # Assert execute was called (it fails after this)
    mock_db_session.execute.assert_called()
    # Assert commit was NOT called
    mock_db_session.commit.assert_not_called()
    # Assert rollback WAS called
    mock_db_session.rollback.assert_called_once()
    # Assert error was logged
    mock_logger.error.assert_called_with("Database error processing message: DB Error")

def test_process_message_json_decode_error():
    """Test handling of JSON decode errors (simulated by bad value)."""
    mock_db_session = MagicMock()
    bad_message = MagicMock()
    bad_message.value = "this is not json" # Simulate bad data
    bad_message.offset = 103

    # process_message should catch the error internally and log it
    # We can't easily mock the json.loads within the lambda, so we test the outcome
    with patch('transformer.logger') as mock_logger:
        process_message(bad_message, mock_db_session)

        # Assert no DB operations attempted
        mock_db_session.execute.assert_not_called()
        mock_db_session.commit.assert_not_called()
        mock_db_session.rollback.assert_not_called() # Rollback not needed if error is before DB ops
        # Check for appropriate log message
        mock_logger.error.assert_called_once()
        assert "Failed to decode JSON message" in mock_logger.error.call_args[0][0]
