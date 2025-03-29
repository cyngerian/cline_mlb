import pytest
import requests
import json
from unittest.mock import MagicMock, patch, ANY # Using unittest.mock integrated with pytest
# Import the functions to test from the poller script
# Assuming poller.py is in the parent directory relative to the tests directory
# Adjust the import path if your structure differs or use package structure later
import sys
import os
# Add the parent directory (api_poller) to the Python path
import datetime as dt # Import datetime for mocking
# Add other imports from poller
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from poller import (
    get_live_game_pks,
    get_game_feed,
    create_kafka_producer,
    send_to_kafka,
    save_raw_json,
    fetch_with_retry, # Import the retry wrapper as well
    KAFKA_BROKER, # Import constants if needed for assertions
    KAFKA_TOPIC,
    RAW_DATA_PATH
)
from kafka import KafkaProducer # Import for type hinting if needed, or mocking
from kafka.errors import KafkaError

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

# --- Tests for fetch_with_retry (Optional but good practice) ---

@patch('poller.requests.get')
@patch('poller.time.sleep')
def test_fetch_with_retry_success_first_try(mock_sleep, mock_get):
    """Test fetch succeeds on the first attempt."""
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {"data": "success"}
    mock_get.return_value = mock_response

    result = fetch_with_retry("http://test.com", "test data")

    mock_get.assert_called_once_with("http://test.com", timeout=15)
    mock_sleep.assert_not_called()
    assert result == {"data": "success"}

@patch('poller.requests.get')
@patch('poller.time.sleep')
def test_fetch_with_retry_success_after_failure(mock_sleep, mock_get):
    """Test fetch succeeds after one failure."""
    mock_success_response = MagicMock()
    mock_success_response.raise_for_status.return_value = None
    mock_success_response.json.return_value = {"data": "success"}
    mock_get.side_effect = [requests.exceptions.Timeout("Timeout"), mock_success_response]

    result = fetch_with_retry("http://test.com", "test data", retries=3, delay=1)

    assert mock_get.call_count == 2
    mock_sleep.assert_called_once_with(1)
    assert result == {"data": "success"}

@patch('poller.requests.get')
@patch('poller.time.sleep')
def test_fetch_with_retry_persistent_failure(mock_sleep, mock_get):
    """Test fetch fails after all retries."""
    mock_get.side_effect = requests.exceptions.RequestException("Persistent error")

    result = fetch_with_retry("http://test.com", "test data", retries=2, delay=1)

    assert mock_get.call_count == 2
    assert mock_sleep.call_count == 1 # Only called after the first failure
    assert result is None

@patch('poller.requests.get')
@patch('poller.time.sleep')
def test_fetch_with_retry_http_4xx_error(mock_sleep, mock_get):
    """Test fetch does not retry on 4xx HTTP errors."""
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
        response=MagicMock(status_code=404, reason="Not Found")
    )
    mock_get.return_value = mock_response

    result = fetch_with_retry("http://test.com", "test data", retries=3, delay=1)

    mock_get.assert_called_once() # Should not retry
    mock_sleep.assert_not_called()
    assert result is None

@patch('poller.requests.get')
@patch('poller.time.sleep')
def test_fetch_with_retry_json_error(mock_sleep, mock_get):
    """Test fetch does not retry on JSON decode errors."""
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.side_effect = json.JSONDecodeError("Invalid JSON", "", 0)
    mock_get.return_value = mock_response

    result = fetch_with_retry("http://test.com", "test data", retries=3, delay=1)

    mock_get.assert_called_once() # Should not retry
    mock_sleep.assert_not_called()
    assert result is None


# --- Tests for get_live_game_pks ---

@patch('poller.fetch_with_retry') # Mock the retry wrapper now
def test_get_live_game_pks_success(mock_fetch):
    """Test successfully finding live game pks."""
    mock_fetch.return_value = MOCK_SCHEDULE_RESPONSE_LIVE
    live_pks = get_live_game_pks()
    mock_fetch.assert_called_once()
    assert live_pks == {745804} # Assert against a set

@patch('poller.fetch_with_retry')
def test_get_live_game_pks_no_live_games(mock_fetch):
    """Test when the schedule API returns no live games."""
    mock_fetch.return_value = MOCK_SCHEDULE_RESPONSE_NONE_LIVE
    live_pks = get_live_game_pks()
    mock_fetch.assert_called_once()
    assert live_pks == set() # Assert against an empty set

@patch('poller.fetch_with_retry')
def test_get_live_game_pks_fetch_error(mock_fetch):
    """Test handling of fetch errors (retries handled within fetch_with_retry)."""
    mock_fetch.return_value = None # fetch_with_retry returns None on failure
    live_pks = get_live_game_pks()
    mock_fetch.assert_called_once()
    assert live_pks == set() # Should return empty set on error

# --- Tests for get_game_feed ---

@patch('poller.fetch_with_retry')
def test_get_game_feed_success(mock_fetch):
    """Test successfully fetching a game feed."""
    game_pk = 745804
    mock_fetch.return_value = MOCK_GAME_FEED_RESPONSE
    feed_data = get_game_feed(game_pk)
    # Assert fetch_with_retry was called correctly
    mock_fetch.assert_called_once_with(
        f"https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live",
        f"game feed for {game_pk}"
    )
    assert feed_data == MOCK_GAME_FEED_RESPONSE

@patch('poller.fetch_with_retry')
def test_get_game_feed_fetch_error(mock_fetch):
    """Test handling fetch errors when fetching game feed."""
    game_pk = 745804
    mock_fetch.return_value = None # fetch_with_retry returns None on failure
    feed_data = get_game_feed(game_pk)
    mock_fetch.assert_called_once()
    assert feed_data is None # Should return None on error

# --- Tests for create_kafka_producer ---

@patch('poller.KafkaProducer') # Mock the KafkaProducer class itself
def test_create_kafka_producer_success(MockKafkaProducer):
    """Test successful Kafka producer creation."""
    mock_producer_instance = MagicMock()
    MockKafkaProducer.return_value = mock_producer_instance
    producer = create_kafka_producer(retries=1) # Test with 1 retry for speed
    MockKafkaProducer.assert_called_once_with(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=ANY, # Use ANY for the lambda function
        acks='all',
        retries=3,
        request_timeout_ms=30000
    )
    assert producer == mock_producer_instance

@patch('poller.KafkaProducer')
@patch('poller.time.sleep') # Mock time.sleep to avoid delays
def test_create_kafka_producer_failure_then_success(mock_sleep, MockKafkaProducer):
    """Test Kafka producer creation failing then succeeding."""
    mock_producer_instance = MagicMock()
    MockKafkaProducer.side_effect = [KafkaError("Connection failed"), mock_producer_instance]
    producer = create_kafka_producer(retries=2, delay=1)
    assert MockKafkaProducer.call_count == 2
    mock_sleep.assert_called_once_with(1) # Check if sleep was called
    assert producer == mock_producer_instance

@patch('poller.KafkaProducer')
@patch('poller.time.sleep')
def test_create_kafka_producer_persistent_failure(mock_sleep, MockKafkaProducer):
    """Test Kafka producer creation failing after all retries."""
    MockKafkaProducer.side_effect = KafkaError("Persistent connection failure")
    producer = create_kafka_producer(retries=2, delay=1)
    assert MockKafkaProducer.call_count == 2
    assert mock_sleep.call_count == 2 # Called after each failure
    assert producer is None

# --- Tests for send_to_kafka ---

def test_send_to_kafka_success():
    """Test successfully sending a message to Kafka."""
    mock_producer = MagicMock(spec=KafkaProducer)
    mock_future = MagicMock()
    mock_producer.send.return_value = mock_future
    data = {"key": "value"}
    game_pk = 12345
    result = send_to_kafka(mock_producer, data, game_pk)
    mock_producer.send.assert_called_once_with(
        KAFKA_TOPIC,
        key=str(game_pk).encode('utf-8'),
        value=data
    )
    assert result is True

def test_send_to_kafka_producer_none():
    """Test behavior when producer is None."""
    result = send_to_kafka(None, {"key": "value"}, 12345)
    assert result is False

def test_send_to_kafka_empty_data():
    """Test behavior with empty data."""
    mock_producer = MagicMock(spec=KafkaProducer)
    result = send_to_kafka(mock_producer, None, 12345)
    mock_producer.send.assert_not_called()
    assert result is False

def test_send_to_kafka_kafka_error():
    """Test handling KafkaError during send."""
    mock_producer = MagicMock(spec=KafkaProducer)
    mock_producer.send.side_effect = KafkaError("Send failed")
    data = {"key": "value"}
    game_pk = 12345
    result = send_to_kafka(mock_producer, data, game_pk)
    mock_producer.send.assert_called_once()
    assert result is False

# Patch flush on the *instance* of the mock producer
def test_send_to_kafka_buffer_error():
    """Test handling BufferError during send."""
    mock_producer = MagicMock(spec=KafkaProducer)
    mock_producer.send.side_effect = BufferError("Buffer full")
    # Mock the flush method directly on the instance
    mock_producer.flush = MagicMock()
    data = {"key": "value"}
    game_pk = 12345

    result = send_to_kafka(mock_producer, data, game_pk)

    mock_producer.send.assert_called_once()
    # Check if the flush method *on the instance* was called
    mock_producer.flush.assert_called_once_with(timeout=5)
    assert result is False


# --- Tests for save_raw_json ---

# Use pytest's tmp_path fixture for temporary directory
@patch('poller.os.makedirs') # Mock os.makedirs
@patch('builtins.open', new_callable=MagicMock) # Mock the built-in open function
@patch('poller.datetime') # Mock datetime to control timestamp
def test_save_raw_json_success(mock_datetime, mock_open, mock_makedirs, tmp_path):
    """Test successfully saving JSON data."""
    mock_file = MagicMock()
    mock_open.return_value.__enter__.return_value = mock_file
    fixed_time = dt.datetime(2024, 3, 30, 12, 0, 0)
    mock_datetime.utcnow.return_value = fixed_time

    with patch('poller.RAW_DATA_PATH', str(tmp_path)):
        game_pk = 98765
        data = {"gamePk": game_pk, "metaData": {"timeStamp": "API_TIMESTAMP"}, "liveData": {}}
        # Use os.path.join for expected paths to match implementation
        expected_path_str = os.path.join(str(tmp_path), str(game_pk))
        expected_filepath_str = os.path.join(expected_path_str, "API_TIMESTAMP.json")

        save_raw_json(data, game_pk)

        mock_makedirs.assert_called_once_with(expected_path_str, exist_ok=True)
        mock_open.assert_called_once_with(expected_filepath_str, 'w', encoding='utf-8')
        # Check that write was called on the file handle mock (REMOVED assert_called_once)
        mock_file.write.assert_called()

@patch('poller.os.makedirs')
@patch('builtins.open', new_callable=MagicMock)
@patch('poller.datetime')
def test_save_raw_json_uses_generated_timestamp(mock_datetime, mock_open, mock_makedirs, tmp_path):
    """Test using generated timestamp when not present in metadata."""
    mock_file = MagicMock()
    mock_open.return_value.__enter__.return_value = mock_file
    fixed_time = dt.datetime(2024, 3, 30, 12, 0, 1)
    mock_datetime.utcnow.return_value = fixed_time
    expected_timestamp = fixed_time.strftime("%Y%m%d_%H%M%S%f")

    with patch('poller.RAW_DATA_PATH', str(tmp_path)):
        game_pk = 98765
        data = {"gamePk": game_pk, "metaData": {}, "liveData": {}} # No timeStamp
        expected_path_str = os.path.join(str(tmp_path), str(game_pk))
        expected_filepath_str = os.path.join(expected_path_str, f"{expected_timestamp}.json")

        save_raw_json(data, game_pk)

        mock_makedirs.assert_called_once_with(expected_path_str, exist_ok=True)
        mock_open.assert_called_once_with(expected_filepath_str, 'w', encoding='utf-8')
        # Check that write was called on the file handle mock (REMOVED assert_called_once)
        mock_file.write.assert_called()

def test_save_raw_json_no_data():
    """Test behavior when data is None."""
    save_raw_json(None, 123)
    # Assert no file operations happened (implicitly tested by not mocking/asserting calls)

@patch('poller.os.makedirs', side_effect=OSError("Permission denied"))
@patch('poller.logger.error') # Mock logger to check output
def test_save_raw_json_os_error(mock_logger_error, mock_makedirs, tmp_path):
    """Test handling OSError during makedirs."""
    with patch('poller.RAW_DATA_PATH', str(tmp_path)):
        game_pk = 98765
        data = {"gamePk": game_pk, "metaData": {}, "liveData": {}}
        expected_path_str = os.path.join(str(tmp_path), str(game_pk))

        save_raw_json(data, game_pk)

        mock_makedirs.assert_called_once_with(expected_path_str, exist_ok=True)
        # Check if the corrected log message was called
        mock_logger_error.assert_called_once_with(
            f"OS error saving raw JSON for game {game_pk} (path: {expected_path_str}): Permission denied"
        )
