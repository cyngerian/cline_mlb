import pytest
from unittest.mock import MagicMock, patch, call, ANY
import json
from datetime import datetime, timezone # Import timezone
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.sql.dml import Insert # To check instance type
from sqlalchemy.exc import SQLAlchemyError # Import for testing specific exception

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

    # Mock the final statement objects that get executed
    mock_final_game_stmt = MagicMock(name="final_game_stmt")
    mock_final_gs_stmt = MagicMock(name="final_gs_stmt")

    # Mock the intermediate objects in the chain
    mock_game_insert_obj = MagicMock(name="game_insert_obj")
    mock_game_values_obj = MagicMock(name="game_values_obj")
    mock_gs_insert_obj = MagicMock(name="gs_insert_obj")
    mock_gs_values_obj = MagicMock(name="gs_values_obj")

    # Configure pg_insert to return the correct insert object based on the table
    def pg_insert_side_effect(table):
        if table == Game:
            return mock_game_insert_obj
        elif table == GameState:
            return mock_gs_insert_obj
        else:
            # In case of unexpected table, fail the test clearly
            pytest.fail(f"Unexpected table passed to pg_insert: {table}")
            return MagicMock() # Return a dummy mock to avoid further errors
    mock_pg_insert.side_effect = pg_insert_side_effect

    # Configure the chain for Game: insert().values().on_conflict_do_update()
    mock_game_insert_obj.values.return_value = mock_game_values_obj
    mock_game_values_obj.on_conflict_do_update.return_value = mock_final_game_stmt

    # Configure the chain for GameState: insert().values().on_conflict_do_update()
    mock_gs_insert_obj.values.return_value = mock_gs_values_obj
    mock_gs_values_obj.on_conflict_do_update.return_value = mock_final_gs_stmt

    # --- Run the function ---
    process_message(MOCK_KAFKA_MESSAGE, mock_db_session)

    # --- Assertions ---
    # Check pg_insert calls
    mock_pg_insert.assert_any_call(Game)
    mock_pg_insert.assert_any_call(GameState)

    # Check values calls with expected data
    expected_game_values = {
        "game_pk": 745804,
        "home_team_id": 111,
        "away_team_id": 147,
        "game_datetime": datetime(2024, 3, 29, 23, 5, 0, tzinfo=timezone.utc), # Use timezone.utc
        "venue_name": "Fenway Park",
        "game_status": "Live",
        "updated_at": ANY
    }
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
    mock_game_insert_obj.values.assert_called_once_with(expected_game_values)
    mock_gs_insert_obj.values.assert_called_once_with(expected_gs_values)

    # Check on_conflict_do_update calls (on the object returned by .values())
    mock_game_values_obj.on_conflict_do_update.assert_called_once()
    game_conflict_args = mock_game_values_obj.on_conflict_do_update.call_args
    assert game_conflict_args[1]['index_elements'] == ['game_pk']
    # Check the 'set_' argument keys
    assert set(game_conflict_args[1]['set_'].keys()) == set(expected_game_values.keys()) - {'game_pk'}

    mock_gs_values_obj.on_conflict_do_update.assert_called_once()
    gs_conflict_args = mock_gs_values_obj.on_conflict_do_update.call_args
    assert gs_conflict_args[1]['index_elements'] == ['game_pk']
    # Check the 'set_' argument keys
    assert set(gs_conflict_args[1]['set_'].keys()) == set(expected_gs_values.keys()) - {'game_pk'}

    # Check execute calls with the final statement objects
    mock_db_session.execute.assert_any_call(mock_final_game_stmt)
    mock_db_session.execute.assert_any_call(mock_final_gs_stmt)
    assert mock_db_session.execute.call_count == 2

    # Check commit/rollback
    mock_db_session.commit.assert_called_once()
    mock_db_session.rollback.assert_not_called()


@patch('transformer.pg_insert')
@patch('transformer.logging.warning') # Patch the specific logging function
def test_process_message_incomplete_data(mock_log_warning, mock_pg_insert):
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
    mock_log_warning.assert_called_once_with(
        f"Incomplete data received for message offset {incomplete_message.offset}. Skipping."
    )

# Test SQLAlchemyError specifically
@patch('transformer.pg_insert', side_effect=SQLAlchemyError("DB Specific Error"))
@patch('transformer.logging.error') # Patch the specific logging function
def test_process_message_sqlalchemy_error(mock_log_error, mock_pg_insert):
    """Test handling of SQLAlchemy errors during processing."""
    mock_db_session = MagicMock()

    process_message(MOCK_KAFKA_MESSAGE, mock_db_session)

    # Assert pg_insert was called (it fails after this)
    mock_pg_insert.assert_called()
    # Assert execute was NOT called
    mock_db_session.execute.assert_not_called()
    # Assert commit was NOT called
    mock_db_session.commit.assert_not_called()
    # Assert rollback WAS called
    mock_db_session.rollback.assert_called_once()
    # Assert specific SQLAlchemyError log message
    mock_log_error.assert_called_with("Database error processing message: DB Specific Error")

# Test generic Exception
@patch('transformer.pg_insert', side_effect=Exception("Generic Error"))
@patch('transformer.logging.error') # Patch the specific logging function
def test_process_message_generic_exception(mock_log_error, mock_pg_insert):
    """Test handling of generic exceptions during processing."""
    mock_db_session = MagicMock()

    process_message(MOCK_KAFKA_MESSAGE, mock_db_session)

    # Assert pg_insert was called (it fails after this)
    mock_pg_insert.assert_called()
    # Assert execute was NOT called
    mock_db_session.execute.assert_not_called()
    # Assert commit was NOT called
    mock_db_session.commit.assert_not_called()
    # Assert rollback WAS called
    mock_db_session.rollback.assert_called_once()
    # Assert generic exception log message
    mock_log_error.assert_called_with("Unexpected error processing message: Generic Error")


def test_process_message_json_decode_error():
    """Test handling of JSON decode errors (simulated by bad value)."""
    mock_db_session = MagicMock()
    bad_message = MagicMock()
    bad_message.value = "this is not json" # Simulate bad data that causes AttributeError later
    bad_message.offset = 103

    # process_message should catch the error internally and log it
    with patch('transformer.logging.error') as mock_log_error: # Patch the specific logging function
        process_message(bad_message, mock_db_session)

        # Assert no DB operations attempted
        mock_db_session.execute.assert_not_called()
        mock_db_session.commit.assert_not_called()
        mock_db_session.rollback.assert_called_once() # Rollback IS called by the generic exception handler
        # Check for appropriate log message (will be AttributeError in this case)
        mock_log_error.assert_called_once()
        assert "Unexpected error processing message: 'str' object has no attribute 'get'" in mock_log_error.call_args[0][0]
