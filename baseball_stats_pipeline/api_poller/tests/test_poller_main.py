import pytest
import json
from unittest.mock import MagicMock, patch, call, ANY
import sys
import os
import time
from datetime import datetime

# Add the parent directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from poller import (
    main,
    get_live_game_pks,
    get_game_feed,
    create_kafka_producer,
    send_to_kafka,
    # save_raw_json, # Replaced
    upload_to_s3, # Added
    create_s3_client, # Added
    KAFKA_BROKER,
    KAFKA_TOPIC,
    # RAW_DATA_PATH, # Removed
    S3_BUCKET_NAME, # Added
    SCHEDULE_POLL_INTERVAL_SECONDS,
    GAME_POLL_INTERVAL_SECONDS
)
from kafka import KafkaProducer
from kafka.errors import KafkaError

# --- Mock Data ---
MOCK_LIVE_GAME_PKS = {745804, 745805}
MOCK_GAME_FEED_DATA = {
    "gamePk": 745804,
    "metaData": {"timeStamp": "20240329_220000"},
    "gameData": {
        "game": {"pk": 745804, "type": "R", "season": "2024"}, # Added game info
        "status": {"abstractGameState": "Live"},
        "teams": {
            "home": {"id": 111, "name": "Boston Red Sox"},
            "away": {"id": 147, "name": "New York Yankees"}
        },
        "datetime": {"dateTime": "2024-03-29T23:05:00Z"},
        "venue": {"name": "Fenway Park"}
    },
    "liveData": {
        "linescore": {
            "currentInning": 3,
            "isTopInning": False,
            "teams": {"home": {"runs": 2}, "away": {"runs": 1}}
        },
        "plays": {"currentPlay": {}}
    }
}

MOCK_FINAL_GAME_FEED_DATA = {
    "gamePk": 745805,
    "metaData": {"timeStamp": "20240329_220000"},
    "gameData": {
        "game": {"pk": 745805, "type": "R", "season": "2024"}, # Added game info
        "status": {"abstractGameState": "Final"},
        "teams": {
            "home": {"id": 111, "name": "Boston Red Sox"},
            "away": {"id": 147, "name": "New York Yankees"}
        },
        "datetime": {"dateTime": "2024-03-29T23:05:00Z"},
        "venue": {"name": "Fenway Park"}
    },
    "liveData": {
        "linescore": {
            "currentInning": 9,
            "isTopInning": False,
            "teams": {"home": {"runs": 5}, "away": {"runs": 3}}
        },
        "plays": {"currentPlay": {}}
    }
}

# --- Tests for main polling loop ---

# Note: Duplicated patch for create_kafka_producer removed below
@patch('poller.create_s3_client') # Added
@patch('poller.create_kafka_producer')
@patch('poller.get_live_game_pks')
@patch('poller.get_game_feed')
@patch('poller.send_to_kafka')
@patch('poller.upload_to_s3') # Changed from save_raw_json
@patch('poller.time.sleep', side_effect=KeyboardInterrupt)  # Interrupt on first sleep call
@patch('poller.time.time')  # Mock time.time to control timing
@patch('poller.logger')  # Mock logger to check output
def test_main_loop_basic_flow(
    mock_logger, mock_time, mock_sleep, mock_upload_to_s3, # Changed
    mock_send_to_kafka, mock_get_game_feed, mock_get_live_game_pks,
    mock_create_kafka_producer, mock_create_s3_client # Added
):
    """Test the basic flow of the main polling loop."""
    # Setup mocks
    mock_producer = MagicMock()
    mock_s3_client_instance = MagicMock() # Added
    mock_create_kafka_producer.return_value = mock_producer
    mock_create_s3_client.return_value = mock_s3_client_instance # Added
    mock_get_live_game_pks.return_value = MOCK_LIVE_GAME_PKS
    mock_get_game_feed.return_value = MOCK_GAME_FEED_DATA
    mock_upload_to_s3.return_value = True # Assume S3 upload succeeds
    mock_send_to_kafka.return_value = True
    mock_time.return_value = 1000  # Fixed time for simplicity

    # Run main (will exit after first iteration due to KeyboardInterrupt)
    main()

    # Verify the flow
    mock_create_kafka_producer.assert_called_once()
    mock_create_s3_client.assert_called_once() # Added
    mock_get_live_game_pks.assert_called_once()

    # Should call get_game_feed for each live game
    assert mock_get_game_feed.call_count == len(MOCK_LIVE_GAME_PKS)
    for game_pk in MOCK_LIVE_GAME_PKS:
        mock_get_game_feed.assert_any_call(game_pk)

    # Should upload and send data for each game
    assert mock_upload_to_s3.call_count == len(MOCK_LIVE_GAME_PKS) # Changed
    assert mock_send_to_kafka.call_count == len(MOCK_LIVE_GAME_PKS)
    for game_pk in MOCK_LIVE_GAME_PKS: # Check calls with correct args
        mock_upload_to_s3.assert_any_call(mock_s3_client_instance, MOCK_GAME_FEED_DATA, game_pk)
        mock_send_to_kafka.assert_any_call(mock_producer, MOCK_GAME_FEED_DATA, game_pk)

    # Should flush the producer
    mock_producer.flush.assert_called_once()
    
    # Should log appropriate messages
    mock_logger.info.assert_any_call("Starting API Poller...")
    mock_logger.info.assert_any_call("Checking schedule for live games...")
    mock_logger.info.assert_any_call(f"Starting polling for new live games: {MOCK_LIVE_GAME_PKS}")
    mock_logger.info.assert_any_call(f"Polling {len(MOCK_LIVE_GAME_PKS)} identified live games: {MOCK_LIVE_GAME_PKS}")
    mock_logger.info.assert_any_call("Shutdown signal received.")
    mock_logger.info.assert_any_call("Shutting down API Poller...")
    mock_logger.info.assert_any_call("Closing Kafka producer...")
    mock_logger.info.assert_any_call("API Poller stopped.")

@patch('poller.create_kafka_producer')
@patch('poller.get_live_game_pks')
@patch('poller.time.sleep', side_effect=KeyboardInterrupt)  # Force exit after first iteration
@patch('poller.logger')  # Mock logger to check output
def test_main_no_live_games(mock_logger, mock_sleep, mock_get_live_game_pks, mock_create_kafka_producer):
    """Test behavior when no live games are found."""
    # Setup mocks
    mock_producer = MagicMock()
    mock_create_kafka_producer.return_value = mock_producer
    mock_get_live_game_pks.return_value = set()  # No live games

    # Run main (will exit after first iteration due to KeyboardInterrupt)
    main()

    # Verify behavior
    mock_create_kafka_producer.assert_called_once()
    mock_get_live_game_pks.assert_called_once()
    
    # Should log appropriate messages
    mock_logger.info.assert_any_call("Starting API Poller...")
    mock_logger.info.assert_any_call("Checking schedule for live games...")
    mock_logger.info.assert_any_call(f"No live games currently identified. Sleeping for {max(10, SCHEDULE_POLL_INTERVAL_SECONDS // 2)} seconds...")
    mock_logger.info.assert_any_call("Shutdown signal received.")

@patch('poller.create_kafka_producer', return_value=None)  # Simulate Kafka connection failure
@patch('poller.logger')  # Mock logger to check output
def test_main_kafka_connection_failure(mock_logger, mock_create_kafka_producer):
    """Test behavior when Kafka connection fails."""
    # Run main
    main()

    # Verify behavior
    mock_create_kafka_producer.assert_called_once()
    
    # Should log critical error and exit
    mock_logger.info.assert_any_call("Starting API Poller...")
    mock_logger.critical.assert_called_once_with("Exiting due to Kafka connection failure.")
    # The "API Poller stopped." message is in a finally block that isn't reached if producer is None
    mock_logger.info.assert_any_call("Starting API Poller...") # Check start message instead

@patch('poller.create_kafka_producer')
# Note: Duplicated patch for create_kafka_producer removed below
@patch('poller.create_s3_client') # Added
@patch('poller.create_kafka_producer')
@patch('poller.get_live_game_pks')
@patch('poller.get_game_feed')
@patch('poller.send_to_kafka')
@patch('poller.upload_to_s3') # Changed
@patch('poller.time.sleep', side_effect=KeyboardInterrupt)  # Interrupt on first sleep call
@patch('poller.time.time')  # Mock time.time to control timing
@patch('poller.logger')  # Mock logger to check output
def test_main_game_status_change(
    mock_logger, mock_time, mock_sleep, mock_upload_to_s3, # Changed
    mock_send_to_kafka, mock_get_game_feed, mock_get_live_game_pks,
    mock_create_kafka_producer, mock_create_s3_client # Added
):
    """Test handling of game status changes."""
    # Setup mocks
    mock_producer = MagicMock()
    mock_s3_client_instance = MagicMock() # Added
    mock_create_kafka_producer.return_value = mock_producer
    mock_create_s3_client.return_value = mock_s3_client_instance # Added
    mock_get_live_game_pks.return_value = {745804, 745805}

    # Return different status for each game
    def get_game_feed_side_effect(game_pk):
        if game_pk == 745804:
            return MOCK_GAME_FEED_DATA  # Live game
        else:
            return MOCK_FINAL_GAME_FEED_DATA  # Final game
    
    mock_get_game_feed.side_effect = get_game_feed_side_effect
    mock_send_to_kafka.return_value = True
    mock_time.return_value = 1000  # Fixed time for simplicity

    # Run main (will exit after first iteration due to KeyboardInterrupt)
    main()

    # Verify behavior
    mock_create_kafka_producer.assert_called_once()
    mock_get_live_game_pks.assert_called_once()

    # Should call get_game_feed twice (once for each game in the first cycle)
    assert mock_get_game_feed.call_count == 2

    # Should upload data for BOTH games (upload happens even for final state)
    assert mock_upload_to_s3.call_count == 2
    # Should send to kafka only ONCE (for the live game)
    assert mock_send_to_kafka.call_count == 1

    # Check the specific calls
    mock_upload_to_s3.assert_any_call(mock_s3_client_instance, MOCK_GAME_FEED_DATA, 745804)
    mock_upload_to_s3.assert_any_call(mock_s3_client_instance, MOCK_FINAL_GAME_FEED_DATA, 745805)
    mock_send_to_kafka.assert_called_once_with(mock_producer, MOCK_GAME_FEED_DATA, 745804)

    # Should log game status change for the final game
    mock_logger.info.assert_any_call("Game 745805 status in feed is now 'Final'. Removing from active polling.")

@patch('poller.create_kafka_producer')
# Note: Duplicated patch for create_kafka_producer removed below
@patch('poller.create_s3_client') # Added
@patch('poller.create_kafka_producer')
@patch('poller.get_live_game_pks')
@patch('poller.get_game_feed')
@patch('poller.send_to_kafka')
@patch('poller.upload_to_s3') # Changed
@patch('poller.time.sleep', side_effect=KeyboardInterrupt)  # Force exit after first iteration
@patch('poller.time.time')  # Mock time.time to control timing
@patch('poller.logger')  # Mock logger to check output
def test_main_game_feed_error(
    mock_logger, mock_time, mock_sleep, mock_upload_to_s3, # Changed
    mock_send_to_kafka, mock_get_game_feed, mock_get_live_game_pks,
    mock_create_kafka_producer, mock_create_s3_client # Added
):
    """Test handling of errors when fetching game feed."""
    # Setup mocks
    mock_producer = MagicMock()
    mock_s3_client_instance = MagicMock() # Added
    mock_create_kafka_producer.return_value = mock_producer
    mock_create_s3_client.return_value = mock_s3_client_instance # Added
    mock_get_live_game_pks.return_value = {745804}
    mock_get_game_feed.return_value = None  # Simulate fetch error
    mock_time.return_value = 1000  # Fixed time for simplicity

    # Run main (will exit after first iteration due to KeyboardInterrupt)
    main()

    # Verify behavior
    mock_create_kafka_producer.assert_called_once()
    mock_get_live_game_pks.assert_called_once()
    mock_get_game_feed.assert_called_once_with(745804)
    
    # Should not upload or send data
    mock_upload_to_s3.assert_not_called() # Changed
    mock_send_to_kafka.assert_not_called()

    # Should log warning
    mock_logger.warning.assert_any_call("No data received for game 745804 after retries. Keeping in polling list for now.")

@patch('poller.create_kafka_producer')
@patch('poller.create_s3_client') # Added
@patch('poller.create_kafka_producer')
@patch('poller.get_live_game_pks')
@patch('poller.get_game_feed')
@patch('poller.send_to_kafka')
@patch('poller.upload_to_s3') # Changed
@patch('poller.time.sleep', side_effect=[None, KeyboardInterrupt])  # Allow one sleep, then interrupt
@patch('poller.time.time')  # Mock time.time to control timing
@patch('poller.logger')  # Mock logger to check output
def test_main_kafka_send_error(
    mock_logger, mock_time, mock_sleep, mock_upload_to_s3, # Changed
    mock_send_to_kafka, mock_get_game_feed, mock_get_live_game_pks,
    mock_create_kafka_producer, mock_create_s3_client # Added
):
    """Test handling of Kafka send errors."""
    # Setup mocks
    mock_producer = MagicMock()
    mock_s3_client_instance = MagicMock() # Added
    mock_create_kafka_producer.return_value = mock_producer
    mock_create_s3_client.return_value = mock_s3_client_instance # Added
    mock_get_live_game_pks.return_value = {745804}
    mock_get_game_feed.return_value = MOCK_GAME_FEED_DATA
    mock_upload_to_s3.return_value = True # Assume S3 succeeds
    mock_send_to_kafka.return_value = False  # Simulate Kafka send error
    # Simulate time passing during polling cycle
    mock_time.side_effect = [
        1000, # Start of schedule check
        1000, # Start of game polling
        1002  # End of game polling (2 seconds elapsed)
    ]

    # Run main (will exit after first iteration due to KeyboardInterrupt)
    main()

    # Verify behavior
    mock_create_kafka_producer.assert_called_once()
    mock_create_s3_client.assert_called_once() # Added
    mock_get_live_game_pks.assert_called_once()
    mock_get_game_feed.assert_called_once_with(745804)
    mock_upload_to_s3.assert_called_once_with(mock_s3_client_instance, MOCK_GAME_FEED_DATA, 745804) # Changed
    mock_send_to_kafka.assert_called_once_with(mock_producer, MOCK_GAME_FEED_DATA, 745804)

    # Should log Kafka failures and correct duration
    mock_logger.info.assert_any_call("Finished polling cycle for 1 games in 2.00 seconds. Kafka failures: 1.")

@patch('poller.create_kafka_producer')
# Note: Duplicated patch for create_kafka_producer removed below
@patch('poller.create_s3_client') # Added
@patch('poller.create_kafka_producer')
@patch('poller.get_live_game_pks')
@patch('poller.time.sleep', side_effect=Exception("Unexpected error"))  # Simulate unexpected error
@patch('poller.logger')  # Mock logger to check output
def test_main_unexpected_error(mock_logger, mock_sleep, mock_get_live_game_pks, mock_create_kafka_producer, mock_create_s3_client): # Signature already correct
    """Test handling of unexpected errors in the main loop."""
    # Setup mocks
    mock_producer = MagicMock()
    mock_s3_client_instance = MagicMock() # Added
    mock_create_kafka_producer.return_value = mock_producer
    mock_create_s3_client.return_value = mock_s3_client_instance # Added
    mock_get_live_game_pks.return_value = {745804}

    # Run main (will exit due to exception)
    main()

    # Verify behavior
    mock_create_kafka_producer.assert_called_once()
    mock_create_s3_client.assert_called_once() # Added
    mock_get_live_game_pks.assert_called_once()

    # Should log exception
    mock_logger.exception.assert_called_once_with("Unhandled exception in main loop: Unexpected error")
    mock_logger.info.assert_any_call("Shutting down API Poller...")
    mock_logger.info.assert_any_call("Closing Kafka producer...")
    mock_logger.info.assert_any_call("API Poller stopped.")

@patch('poller.create_kafka_producer')
# Note: Duplicated patch for create_kafka_producer removed below
@patch('poller.create_s3_client') # Added
@patch('poller.create_kafka_producer')
@patch('poller.get_live_game_pks', side_effect=Exception("Schedule API error"))  # Simulate schedule API error
@patch('poller.time.sleep', side_effect=KeyboardInterrupt)  # Force exit after first iteration
@patch('poller.logger')  # Mock logger to check output
def test_main_schedule_api_error(mock_logger, mock_sleep, mock_get_live_game_pks, mock_create_kafka_producer, mock_create_s3_client): # Signature already correct
    """Test handling of errors when checking the schedule."""
    # Setup mocks
    mock_producer = MagicMock()
    mock_s3_client_instance = MagicMock() # Added
    mock_create_kafka_producer.return_value = mock_producer
    mock_create_s3_client.return_value = mock_s3_client_instance # Added

    # Run main (will exit after first iteration due to KeyboardInterrupt)
    main()

    # Verify behavior
    mock_create_kafka_producer.assert_called_once()
    mock_create_s3_client.assert_called_once() # Added
    mock_get_live_game_pks.assert_called_once()

    # Should log error
    mock_logger.error.assert_any_call("Error during schedule check: Schedule API error. Will retry later.")
    mock_logger.info.assert_any_call(f"No live games currently identified. Sleeping for {max(10, SCHEDULE_POLL_INTERVAL_SECONDS // 2)} seconds...")

@patch('poller.create_kafka_producer')
# Note: Duplicated patch for create_kafka_producer removed below
@patch('poller.create_s3_client') # Added
@patch('poller.create_kafka_producer')
@patch('poller.get_live_game_pks')
@patch('poller.get_game_feed')
@patch('poller.send_to_kafka')
@patch('poller.upload_to_s3') # Changed
@patch('poller.time.sleep', side_effect=[None, KeyboardInterrupt])  # Allow one sleep, then exit
@patch('poller.time.time')  # Mock time.time to control timing
@patch('poller.logger')  # Mock logger to check output
def test_main_game_polling_timing(
    mock_logger, mock_time, mock_sleep, mock_upload_to_s3, # Changed
    mock_send_to_kafka, mock_get_game_feed, mock_get_live_game_pks,
    mock_create_kafka_producer, mock_create_s3_client # Added
):
    """Test timing logic for game polling."""
    # Setup mocks
    mock_producer = MagicMock()
    mock_s3_client_instance = MagicMock() # Added
    mock_create_kafka_producer.return_value = mock_producer
    mock_create_s3_client.return_value = mock_s3_client_instance # Added
    mock_get_live_game_pks.return_value = {745804}
    mock_get_game_feed.return_value = MOCK_GAME_FEED_DATA
    mock_upload_to_s3.return_value = True # Assume S3 succeeds
    mock_send_to_kafka.return_value = True
    
    # Simulate time passing during polling
    mock_time.side_effect = [
        1000,  # Initial time
        1000,  # Time at start of schedule check
        1005,  # Time after schedule check (5 seconds elapsed)
        1005,  # Time at start of game polling
        1010   # Time after game polling (5 seconds elapsed)
    ]

    # Run main (will exit after second iteration due to KeyboardInterrupt)
    main()

    # Verify behavior
    assert mock_get_live_game_pks.call_count == 1
    # The loop runs twice before interrupt
    assert mock_get_game_feed.call_count == 2

    # Should sleep for the correct amount of time
    # GAME_POLL_INTERVAL_SECONDS - poll_duration = 30 - 5 = 25
    mock_sleep.assert_any_call(25)

@patch('poller.create_kafka_producer')
@patch('poller.get_live_game_pks')
@patch('poller.time.sleep', side_effect=[None, None, KeyboardInterrupt])  # Allow two sleeps, then exit
@patch('poller.time.time')  # Mock time.time to control timing
@patch('poller.logger')  # Mock logger to check output
def test_main_schedule_check_timing(
    mock_logger, mock_time, mock_sleep, mock_get_live_game_pks, mock_create_kafka_producer
):
    """Test timing logic for schedule checks."""
    # Setup mocks
    mock_producer = MagicMock()
    mock_create_kafka_producer.return_value = mock_producer
    mock_get_live_game_pks.return_value = set()  # No live games
    
    # Simulate time passing
    mock_time.side_effect = [
        1000,                                  # Initial time
        1000,                                  # Time at start of first schedule check
        1000 + SCHEDULE_POLL_INTERVAL_SECONDS  # Time after sleep (exactly at next schedule check)
    ]

    # Run main (will exit after second iteration due to KeyboardInterrupt)
    main()

    # Verify behavior
    assert mock_get_live_game_pks.call_count == 2  # Should check schedule twice
    
    # Should sleep for the correct amount of time
    mock_sleep.assert_any_call(max(10, SCHEDULE_POLL_INTERVAL_SECONDS // 2))  # Sleep when no games

@patch('poller.create_kafka_producer')
# Note: Duplicated patch for create_kafka_producer removed below
@patch('poller.create_s3_client') # Added
@patch('poller.create_kafka_producer')
@patch('poller.get_live_game_pks')
@patch('poller.get_game_feed')
@patch('poller.send_to_kafka')
@patch('poller.upload_to_s3') # Changed
@patch('poller.time.sleep', side_effect=[None, KeyboardInterrupt])  # Allow one sleep, then interrupt on the second
@patch('poller.time.time')  # Mock time.time to control timing
@patch('poller.logger')  # Mock logger to check output
def test_main_game_status_tracking(
    mock_logger, mock_time, mock_sleep, mock_upload_to_s3, # Changed
    mock_send_to_kafka, mock_get_game_feed, mock_get_live_game_pks,
    mock_create_kafka_producer, mock_create_s3_client # Added
):
    """Test tracking of game status changes across polling cycles."""
    # Setup mocks
    mock_producer = MagicMock()
    mock_s3_client_instance = MagicMock() # Added
    mock_create_kafka_producer.return_value = mock_producer
    mock_create_s3_client.return_value = mock_s3_client_instance # Added

    # First cycle: two live games
    # Second cycle: one game finished, one still live
    mock_get_live_game_pks.side_effect = [
        {745804, 745805},  # First schedule check
        {745804}           # Second schedule check (745805 no longer live)
    ]
    
    mock_get_game_feed.return_value = MOCK_GAME_FEED_DATA
    mock_upload_to_s3.return_value = True # Assume S3 succeeds
    mock_send_to_kafka.return_value = True

    # Simulate time passing to trigger two schedule checks
    mock_time.side_effect = [
        1000,  # Time at start of first schedule check
        1000,  # Time after first schedule check
        2000,  # Time at start of second schedule check (well after interval)
        2000   # Time after second schedule check
    ]

    # Run main (will exit after second iteration due to KeyboardInterrupt)
    main()

    # Verify behavior
    assert mock_get_live_game_pks.call_count == 2
    
    # First cycle runs fully, second cycle starts schedule check but gets interrupted
    # First cycle polls two games
    assert mock_get_game_feed.call_count == 2
    assert mock_upload_to_s3.call_count == 2 # Uploads both games in first cycle
    assert mock_send_to_kafka.call_count == 2 # Sends both games in first cycle

    # Should log game status changes from the second schedule check
    mock_logger.info.assert_any_call("Starting polling for new live games: {745804, 745805}")
    mock_logger.info.assert_any_call("Stopping polling for finished/non-live games: {745805}")
