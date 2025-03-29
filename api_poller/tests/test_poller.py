import pytest
import requests
from unittest.mock import MagicMock, patch # Using unittest.mock integrated with pytest
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

# --- Tests for create_kafka_producer ---

@patch('poller.KafkaProducer') # Mock the KafkaProducer class itself
def test_create_kafka_producer_success(MockKafkaProducer):
    """Test successful Kafka producer creation."""
    mock_producer_instance = MagicMock()
    MockKafkaProducer.return_value = mock_producer_instance

    producer = create_kafka_producer(retries=1) # Test with 1 retry for speed

    MockKafkaProducer.assert_called_once_with(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=pytest.approx(lambda v: json.dumps(v).encode('utf-8')), # Check serializer type
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
    # Simulate failure on first call, success on second
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
    # If you were blocking with future.get(), you'd assert that too
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

@patch('poller.KafkaProducer.flush') # Mock flush specifically if needed
def test_send_to_kafka_buffer_error(mock_flush):
    """Test handling BufferError during send."""
    mock_producer = MagicMock(spec=KafkaProducer)
    mock_producer.send.side_effect = BufferError("Buffer full")
    data = {"key": "value"}
    game_pk = 12345

    result = send_to_kafka(mock_producer, data, game_pk)

    mock_producer.send.assert_called_once()
    mock_flush.assert_called_once_with(timeout=5) # Check if flush was attempted
    assert result is False


# --- Tests for save_raw_json ---

# Use pytest's tmp_path fixture for temporary directory
@patch('poller.os.makedirs') # Mock os.makedirs
@patch('builtins.open', new_callable=MagicMock) # Mock the built-in open function
@patch('poller.datetime') # Mock datetime to control timestamp
def test_save_raw_json_success(mock_datetime, mock_open, mock_makedirs, tmp_path):
    """Test successfully saving JSON data."""
    # Configure mocks
    mock_file = MagicMock()
    mock_open.return_value.__enter__.return_value = mock_file
    fixed_time = dt.datetime(2024, 3, 30, 12, 0, 0)
    mock_datetime.utcnow.return_value = fixed_time
    expected_timestamp = fixed_time.strftime("%Y%m%d_%H%M%S%f")

    # Override RAW_DATA_PATH for this test using tmp_path
    with patch('poller.RAW_DATA_PATH', str(tmp_path)):
        game_pk = 98765
        data = {"gamePk": game_pk, "metaData": {"timeStamp": "API_TIMESTAMP"}, "liveData": {}}
        expected_path = tmp_path / str(game_pk)
        expected_filepath = expected_path / "API_TIMESTAMP.json" # Uses timestamp from data

        save_raw_json(data, game_pk)

        # Assertions
        mock_makedirs.assert_called_once_with(expected_path, exist_ok=True)
        mock_open.assert_called_once_with(expected_filepath, 'w', encoding='utf-8')
        # Check if json.dump was called correctly on the mock file handle
        mock_file.write.assert_called_once()
        # More detailed check: parse the string written to the mock file
        written_content = mock_file.write.call_args[0][0]
        assert json.loads(written_content) == data

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
        expected_path = tmp_path / str(game_pk)
        expected_filepath = expected_path / f"{expected_timestamp}.json"

        save_raw_json(data, game_pk)

        mock_makedirs.assert_called_once_with(expected_path, exist_ok=True)
        mock_open.assert_called_once_with(expected_filepath, 'w', encoding='utf-8')
        mock_file.write.assert_called_once()

def test_save_raw_json_no_data():
    """Test behavior when data is None."""
    # No mocks needed as it should exit early
    save_raw_json(None, 123)
    # Assert no file operations happened (implicitly tested by not mocking/asserting calls)

@patch('poller.os.makedirs', side_effect=OSError("Permission denied"))
def test_save_raw_json_os_error(mock_makedirs, tmp_path):
    """Test handling OSError during makedirs."""
    with patch('poller.RAW_DATA_PATH', str(tmp_path)):
        game_pk = 98765
        data = {"gamePk": game_pk, "metaData": {}, "liveData": {}}
        # Expect function to catch OSError and log, not raise
        save_raw_json(data, game_pk)
        mock_makedirs.assert_called_once() # Ensure it was called

# --- Tests for get_game_feed_json_error (Example of keeping existing tests) ---
@patch('poller.requests.get')
def test_get_game_feed_json_error_existing(mock_get): # Renamed slightly to avoid conflict
    """Test handling JSON errors when fetching game feed."""
    game_pk = 745804
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.side_effect = requests.exceptions.JSONDecodeError("Bad JSON", "", 0)
    mock_get.return_value = mock_response

    feed_data = get_game_feed(game_pk)

    mock_get.assert_called_once()
    assert feed_data is None # Should return None on error
