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
    # save_raw_json, # Function replaced
    upload_to_s3, # Import new function
    create_s3_client, # Import new function
    fetch_with_retry, # Import the retry wrapper as well
    KAFKA_BROKER, # Import constants if needed for assertions
    KAFKA_TOPIC,
    # RAW_DATA_PATH # Constant removed
    S3_BUCKET_NAME # Import S3 constant
)
import boto3 # Import for mocking
from botocore.exceptions import ClientError
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
    "gameData": {
        "game": {"pk": 745804, "type": "R", "season": "2024"}, # Added game info
        "status": {"abstractGameState": "Live"}
    },
    "liveData": {"plays": {"currentPlay": {}}}
}

# Mock data for S3 client creation test
MOCK_S3_CLIENT_CONFIG = {
    'endpoint_url': 'http://minio:9000',
    'aws_access_key_id': 'testkey',
    'aws_secret_access_key': 'testsecret',
    'region_name': 'us-east-1'
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

# --- Tests for create_s3_client ---

@patch('poller.boto3.client')
def test_create_s3_client_success(mock_boto_client):
    """Test successful S3 client creation and connection check."""
    mock_s3_instance = MagicMock()
    mock_boto_client.return_value = mock_s3_instance
    mock_s3_instance.head_bucket.return_value = {} # Simulate successful head_bucket

    # Patch environment variables for the test duration
    with patch.dict(os.environ, MOCK_S3_CLIENT_CONFIG, clear=True):
        client = create_s3_client(retries=1)

    mock_boto_client.assert_called_once_with(
        's3',
        endpoint_url=MOCK_S3_CLIENT_CONFIG['endpoint_url'],
        aws_access_key_id=MOCK_S3_CLIENT_CONFIG['aws_access_key_id'],
        aws_secret_access_key=MOCK_S3_CLIENT_CONFIG['aws_secret_access_key'],
        region_name=MOCK_S3_CLIENT_CONFIG['region_name']
    )
    mock_s3_instance.head_bucket.assert_called_once_with(Bucket=S3_BUCKET_NAME)
    assert client == mock_s3_instance

@patch('poller.boto3.client')
@patch('poller.time.sleep')
def test_create_s3_client_retry_then_success(mock_sleep, mock_boto_client):
    """Test S3 client creation succeeding after a retry."""
    mock_s3_instance = MagicMock()
    mock_s3_instance.head_bucket.return_value = {}
    # Simulate connection error first, then success
    mock_boto_client.side_effect = [
        ClientError({'Error': {'Code': 'SomeError', 'Message': 'Details'}}, 'OperationName'),
        mock_s3_instance
    ]

    with patch.dict(os.environ, MOCK_S3_CLIENT_CONFIG, clear=True):
        client = create_s3_client(retries=2, delay=1)

    assert mock_boto_client.call_count == 2
    mock_sleep.assert_called_once_with(1)
    assert client == mock_s3_instance

@patch('poller.boto3.client')
@patch('poller.time.sleep')
def test_create_s3_client_no_such_bucket(mock_sleep, mock_boto_client):
    """Test S3 client creation when bucket doesn't exist."""
    mock_s3_instance = MagicMock()
    mock_s3_instance.head_bucket.side_effect = ClientError(
        {'Error': {'Code': 'NoSuchBucket', 'Message': 'Bucket does not exist'}},
        'HeadBucket'
    )
    mock_boto_client.return_value = mock_s3_instance

    with patch.dict(os.environ, MOCK_S3_CLIENT_CONFIG, clear=True):
        client = create_s3_client(retries=1)

    mock_s3_instance.head_bucket.assert_called_once()
    mock_sleep.assert_not_called() # Should not retry on NoSuchBucket by default
    assert client is None

@patch('poller.boto3.client')
@patch('poller.time.sleep')
def test_create_s3_client_auth_error(mock_sleep, mock_boto_client):
    """Test S3 client creation with authentication error."""
    mock_boto_client.side_effect = ClientError(
        {'Error': {'Code': 'InvalidAccessKeyId', 'Message': 'Invalid Key'}},
        'OperationName'
    )

    with patch.dict(os.environ, MOCK_S3_CLIENT_CONFIG, clear=True):
        client = create_s3_client(retries=2, delay=1)

    assert mock_boto_client.call_count == 2
    assert mock_sleep.call_count == 2 # Retries on auth error
    assert client is None


# --- Tests for upload_to_s3 ---

def test_upload_to_s3_success():
    """Test successfully uploading JSON data to S3."""
    mock_s3_client = MagicMock()
    game_pk = MOCK_GAME_FEED_RESPONSE['gamePk']
    data = MOCK_GAME_FEED_RESPONSE
    timestamp = data['metaData']['timeStamp']
    season = data['gameData']['game']['season']
    game_type = 'regular' # Mapped from 'R'
    status = 'live' # Mapped from 'Live'

    expected_key = f"{season}/{game_type}/{status}/{game_pk}/{timestamp}.json"
    expected_body = json.dumps(data, ensure_ascii=False, indent=2).encode('utf-8')

    result = upload_to_s3(mock_s3_client, data, game_pk)

    mock_s3_client.put_object.assert_called_once_with(
        Bucket=S3_BUCKET_NAME,
        Key=expected_key,
        Body=expected_body,
        ContentType='application/json'
    )
    assert result is True

def test_upload_to_s3_client_none():
    """Test behavior when S3 client is None."""
    result = upload_to_s3(None, MOCK_GAME_FEED_RESPONSE, 123)
    assert result is False

def test_upload_to_s3_no_data():
    """Test behavior when data is None."""
    mock_s3_client = MagicMock()
    result = upload_to_s3(mock_s3_client, None, 123)
    mock_s3_client.put_object.assert_not_called()
    assert result is False

@patch('poller.datetime')
def test_upload_to_s3_uses_generated_timestamp(mock_datetime):
    """Test using generated timestamp when not present in metadata."""
    mock_s3_client = MagicMock()
    fixed_time = dt.datetime(2024, 3, 30, 12, 0, 1)
    mock_datetime.utcnow.return_value = fixed_time
    expected_timestamp = fixed_time.strftime("%Y%m%d_%H%M%S%f")

    game_pk = 98765
    # Data missing metaData.timeStamp and game info for path
    data = {"gamePk": game_pk, "gameData": {"status": {"abstractGameState": "Preview"}}, "liveData": {}}
    season = "unknown_season"
    game_type = "unknown_type_U"
    status = "preview"
    expected_key = f"{season}/{game_type}/{status}/{game_pk}/{expected_timestamp}.json"
    expected_body = json.dumps(data, ensure_ascii=False, indent=2).encode('utf-8')

    result = upload_to_s3(mock_s3_client, data, game_pk)

    mock_s3_client.put_object.assert_called_once_with(
        Bucket=S3_BUCKET_NAME,
        Key=expected_key,
        Body=expected_body,
        ContentType='application/json'
    )
    assert result is True

def test_upload_to_s3_client_error():
    """Test handling ClientError during S3 upload."""
    mock_s3_client = MagicMock()
    mock_s3_client.put_object.side_effect = ClientError(
        {'Error': {'Code': 'SomeS3Error', 'Message': 'Upload failed'}},
        'PutObject'
    )
    game_pk = MOCK_GAME_FEED_RESPONSE['gamePk']
    data = MOCK_GAME_FEED_RESPONSE

    result = upload_to_s3(mock_s3_client, data, game_pk)

    mock_s3_client.put_object.assert_called_once() # Check it was attempted
    assert result is False
