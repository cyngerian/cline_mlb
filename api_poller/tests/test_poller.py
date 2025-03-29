import pytest
import requests
from unittest.mock import MagicMock, patch # Using unittest.mock integrated with pytest
# Import the functions to test from the poller script
# Assuming poller.py is in the parent directory relative to the tests directory
# Adjust the import path if your structure differs or use package structure later
import sys
import os
# Add the parent directory (api_poller) to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from poller import get_live_game_pks, get_game_feed

# --- Test Data ---
MOCK_SCHEDULE_RESPONSE_LIVE = {
    "totalGames": 1,
    "dates": [{
        "date": "2024-03-29",
        "games": [{
            "gamePk": 745804,
            "status": {"abstractGameState": "Live"}
        }, {
            "gamePk": 745805,
            "status": {"abstractGameState": "Preview"}
        }]
    }]
}

MOCK_SCHEDULE_RESPONSE_NONE_LIVE = {
    "totalGames": 1,
    "dates": [{
        "date": "2024-03-29",
        "games": [{
            "gamePk": 745805,
            "status": {"abstractGameState": "Final"}
        }]
    }]
}

MOCK_GAME_FEED_RESPONSE = {
    "gamePk": 745804,
    "metaData": {"timeStamp": "20240329_220000"},
    "gameData": {"status": {"abstractGameState": "Live"}},
    "liveData": {"plays": {"currentPlay": {}}}
}

# --- Tests for get_live_game_pks ---

@patch('poller.requests.get') # Mock requests.get within the poller module
def test_get_live_game_pks_success(mock_get):
    """Test successfully finding live game pks."""
    # Configure the mock response
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = MOCK_SCHEDULE_RESPONSE_LIVE
    mock_get.return_value = mock_response

    live_pks = get_live_game_pks()

    # Assertions
    mock_get.assert_called_once() # Check if requests.get was called
    assert live_pks == [745804] # Check if the correct pk was returned

@patch('poller.requests.get')
def test_get_live_game_pks_no_live_games(mock_get):
    """Test when the schedule API returns no live games."""
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = MOCK_SCHEDULE_RESPONSE_NONE_LIVE
    mock_get.return_value = mock_response

    live_pks = get_live_game_pks()

    mock_get.assert_called_once()
    assert live_pks == []

@patch('poller.requests.get')
def test_get_live_game_pks_api_error(mock_get):
    """Test handling of API request errors."""
    # Configure mock to raise an exception
    mock_get.side_effect = requests.exceptions.RequestException("API unavailable")

    live_pks = get_live_game_pks()

    mock_get.assert_called_once()
    assert live_pks == [] # Should return empty list on error

@patch('poller.requests.get')
def test_get_live_game_pks_json_error(mock_get):
    """Test handling of JSON decoding errors."""
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    # Configure mock to raise JSONDecodeError when .json() is called
    mock_response.json.side_effect = requests.exceptions.JSONDecodeError("Invalid JSON", "", 0)
    mock_get.return_value = mock_response

    live_pks = get_live_game_pks()

    mock_get.assert_called_once()
    assert live_pks == [] # Should return empty list on error

# --- Tests for get_game_feed ---

@patch('poller.requests.get')
def test_get_game_feed_success(mock_get):
    """Test successfully fetching a game feed."""
    game_pk = 745804
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = MOCK_GAME_FEED_RESPONSE
    mock_get.return_value = mock_response

    feed_data = get_game_feed(game_pk)

    mock_get.assert_called_once_with(f"https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live", timeout=15)
    assert feed_data == MOCK_GAME_FEED_RESPONSE

@patch('poller.requests.get')
def test_get_game_feed_api_error(mock_get):
    """Test handling API errors when fetching game feed."""
    game_pk = 745804
    mock_get.side_effect = requests.exceptions.RequestException("Feed unavailable")

    feed_data = get_game_feed(game_pk)

    mock_get.assert_called_once()
    assert feed_data is None # Should return None on error

@patch('poller.requests.get')
def test_get_game_feed_json_error(mock_get):
    """Test handling JSON errors when fetching game feed."""
    game_pk = 745804
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.side_effect = requests.exceptions.JSONDecodeError("Bad JSON", "", 0)
    mock_get.return_value = mock_response

    feed_data = get_game_feed(game_pk)

    mock_get.assert_called_once()
    assert feed_data is None # Should return None on error
