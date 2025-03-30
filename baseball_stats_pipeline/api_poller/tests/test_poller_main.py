import pytest
import json
from unittest.mock import MagicMock, patch, call, ANY
import sys
import os
import time
from datetime import datetime, timedelta # Added

# Add the parent directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from poller import (
    main,
    get_game_pks_to_poll, # Use renamed function
    get_game_feed,
    create_kafka_producer,
    send_to_kafka,
    upload_to_s3,
    create_s3_client,
    run_live_polling, # Import new function
    run_backfill, # Import new function
    KAFKA_BROKER,
    KAFKA_TOPIC,
    S3_BUCKET_NAME,
    SCHEDULE_POLL_INTERVAL_SECONDS,
    GAME_POLL_INTERVAL_SECONDS,
    parser # Import parser to mock parse_args
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

# --- Tests for main function ---

@patch('poller.create_s3_client')
@patch('poller.create_kafka_producer')
@patch('poller.run_live_polling') # Patch the live polling function
@patch('poller.run_backfill') # Patch the backfill function
@patch('poller.parser.parse_args') # Patch argument parsing
@patch('poller.logger')
def test_main_calls_live_polling_by_default(
    mock_logger, mock_parse_args, mock_run_backfill, mock_run_live_polling,
    mock_create_kafka_producer, mock_create_s3_client
):
    """Test that main calls run_live_polling when no backfill args are given."""
    # Setup mocks
    mock_producer = MagicMock()
    mock_s3_client = MagicMock()
    mock_create_kafka_producer.return_value = mock_producer
    mock_create_s3_client.return_value = mock_s3_client
    # Simulate no backfill args
    mock_parse_args.return_value = MagicMock(backfill_days=0)

    # Run main
    main()

    # Verify initialization and correct function call
    mock_create_kafka_producer.assert_called_once()
    mock_create_s3_client.assert_called_once()
    mock_parse_args.assert_called_once()
    mock_run_live_polling.assert_called_once_with(mock_producer, mock_s3_client)
    mock_run_backfill.assert_not_called()
    mock_logger.info.assert_any_call("Initializing API Poller...")
    # Check shutdown logs
    mock_logger.info.assert_any_call("Shutting down API Poller...")
    mock_logger.info.assert_any_call("Closing Kafka producer...")
    mock_logger.info.assert_any_call("API Poller stopped.")
    mock_producer.close.assert_called_once()


@patch('poller.create_s3_client')
@patch('poller.create_kafka_producer')
@patch('poller.run_live_polling') # Patch the live polling function
@patch('poller.run_backfill') # Patch the backfill function
@patch('poller.parser.parse_args') # Patch argument parsing
@patch('poller.logger')
def test_main_calls_backfill(
    mock_logger, mock_parse_args, mock_run_backfill, mock_run_live_polling,
    mock_create_kafka_producer, mock_create_s3_client
):
    """Test that main calls run_backfill when backfill args are given."""
    # Setup mocks
    mock_producer = MagicMock()
    mock_s3_client = MagicMock()
    mock_create_kafka_producer.return_value = mock_producer
    mock_create_s3_client.return_value = mock_s3_client
    # Simulate backfill args
    backfill_days = 2
    mock_parse_args.return_value = MagicMock(backfill_days=backfill_days)

    # Run main
    main()

    # Verify initialization and correct function call
    mock_create_kafka_producer.assert_called_once()
    mock_create_s3_client.assert_called_once()
    mock_parse_args.assert_called_once()
    mock_run_backfill.assert_called_once_with(backfill_days, mock_producer, mock_s3_client)
    mock_run_live_polling.assert_not_called()
    mock_logger.info.assert_any_call("Initializing API Poller...")
    # Check shutdown logs
    mock_logger.info.assert_any_call("Shutting down API Poller...")
    mock_logger.info.assert_any_call("Closing Kafka producer...")
    mock_logger.info.assert_any_call("API Poller stopped.")
    mock_producer.close.assert_called_once()


@patch('poller.create_s3_client', return_value=None) # Simulate S3 connection failure
@patch('poller.create_kafka_producer')
@patch('poller.run_live_polling')
@patch('poller.run_backfill')
@patch('poller.parser.parse_args')
@patch('poller.logger')
def test_main_s3_connection_failure(
    mock_logger, mock_parse_args, mock_run_backfill, mock_run_live_polling,
    mock_create_kafka_producer, mock_create_s3_client
):
    """Test behavior when S3 connection fails."""
    # Setup mocks
    mock_producer = MagicMock() # Kafka producer might still be created
    mock_create_kafka_producer.return_value = mock_producer
    mock_parse_args.return_value = MagicMock(backfill_days=0)

    # Run main
    main()

    # Verify behavior
    mock_create_s3_client.assert_called_once()
    mock_create_kafka_producer.assert_called_once() # Kafka creation is attempted
    mock_run_live_polling.assert_not_called()
    mock_run_backfill.assert_not_called()

    # Should log critical error and exit
    mock_logger.critical.assert_called_once_with("Exiting due to Kafka or S3 connection failure.")
    # Check that shutdown logs are NOT called because main returns early
    assert call("Shutting down API Poller...") not in mock_logger.info.call_args_list
    assert call("Closing Kafka producer...") not in mock_logger.info.call_args_list
    assert call("API Poller stopped.") not in mock_logger.info.call_args_list
    mock_producer.close.assert_not_called() # Close is not called in finally block


@patch('poller.create_s3_client')
@patch('poller.create_kafka_producer', return_value=None) # Simulate Kafka connection failure
@patch('poller.run_live_polling')
@patch('poller.run_backfill')
@patch('poller.parser.parse_args')
@patch('poller.logger')
def test_main_kafka_connection_failure(
    mock_logger, mock_parse_args, mock_run_backfill, mock_run_live_polling,
    mock_create_kafka_producer, mock_create_s3_client
):
    """Test behavior when Kafka connection fails."""
    # Setup mocks
    mock_s3_client = MagicMock() # S3 client might still be created
    mock_create_s3_client.return_value = mock_s3_client
    mock_parse_args.return_value = MagicMock(backfill_days=0)

    # Run main
    main()

    # Verify behavior
    mock_create_s3_client.assert_called_once()
    mock_create_kafka_producer.assert_called_once()
    mock_run_live_polling.assert_not_called()
    mock_run_backfill.assert_not_called()

    # Should log critical error and exit
    mock_logger.critical.assert_called_once_with("Exiting due to Kafka or S3 connection failure.")
    # Check that shutdown logs are NOT called because main returns early
    assert call("Shutting down API Poller...") not in mock_logger.info.call_args_list
    assert call("Closing Kafka producer...") not in mock_logger.info.call_args_list
    assert call("API Poller stopped.") not in mock_logger.info.call_args_list


# --- Tests for run_live_polling function ---
# (These tests are similar to the old main loop tests, but target run_live_polling)

@patch('poller.get_game_pks_to_poll')
@patch('poller.get_game_feed')
@patch('poller.send_to_kafka')
@patch('poller.upload_to_s3')
@patch('poller.time.sleep', side_effect=KeyboardInterrupt) # Interrupt on first sleep
@patch('poller.time.time')
@patch('poller.logger')
def test_run_live_polling_basic_flow(
    mock_logger, mock_time, mock_sleep, mock_upload_to_s3,
    mock_send_to_kafka, mock_get_game_feed, mock_get_game_pks_to_poll
):
    """Test the basic flow of the live polling loop."""
    mock_producer = MagicMock()
    mock_s3_client = MagicMock()
    mock_get_game_pks_to_poll.return_value = MOCK_LIVE_GAME_PKS
    mock_get_game_feed.return_value = MOCK_GAME_FEED_DATA
    mock_upload_to_s3.return_value = True
    mock_send_to_kafka.return_value = True
    mock_time.return_value = 1000

    # Run the live polling function directly
    run_live_polling(mock_producer, mock_s3_client)

    # Verify the flow within the loop
    mock_get_game_pks_to_poll.assert_called_once_with() # Called without args
    assert mock_get_game_feed.call_count == len(MOCK_LIVE_GAME_PKS)
    assert mock_upload_to_s3.call_count == len(MOCK_LIVE_GAME_PKS)
    assert mock_send_to_kafka.call_count == len(MOCK_LIVE_GAME_PKS)
    mock_producer.flush.assert_called_once()
    mock_logger.info.assert_any_call("Starting live polling loop...")
    mock_logger.info.assert_any_call("Shutdown signal received.") # From KeyboardInterrupt


@patch('poller.get_game_pks_to_poll')
@patch('poller.time.sleep', side_effect=KeyboardInterrupt)
@patch('poller.logger')
def test_run_live_polling_no_live_games(mock_logger, mock_sleep, mock_get_game_pks_to_poll):
    """Test live polling loop when no live games are found."""
    mock_producer = MagicMock()
    mock_s3_client = MagicMock()
    mock_get_game_pks_to_poll.return_value = set()

    run_live_polling(mock_producer, mock_s3_client)

    mock_get_game_pks_to_poll.assert_called_once_with()
    mock_logger.info.assert_any_call(f"No live games currently identified. Sleeping for {max(10, SCHEDULE_POLL_INTERVAL_SECONDS // 2)} seconds...")
    mock_logger.info.assert_any_call("Shutdown signal received.")


# --- Tests for run_backfill function ---

@patch('poller.get_game_pks_to_poll')
@patch('poller.get_game_feed')
@patch('poller.send_to_kafka')
@patch('poller.upload_to_s3')
@patch('poller.time.sleep') # Mock sleep within the loop
@patch('poller.logger')
def test_run_backfill_basic_flow(
    mock_logger, mock_sleep, mock_upload_to_s3, mock_send_to_kafka,
    mock_get_game_feed, mock_get_game_pks_to_poll
):
    """Test the basic flow of the backfill function."""
    mock_producer = MagicMock()
    mock_s3_client = MagicMock()
    backfill_days = 1
    # Simulate finding past games
    past_game_pks = {745800, 745801}
    mock_get_game_pks_to_poll.return_value = past_game_pks
    # Simulate getting feed data for those games
    mock_get_game_feed.return_value = MOCK_FINAL_GAME_FEED_DATA # Use final state data
    mock_upload_to_s3.return_value = True
    mock_send_to_kafka.return_value = True

    # Run the backfill function
    run_backfill(backfill_days, mock_producer, mock_s3_client)

    # Verify schedule call with dates
    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=backfill_days)
    mock_get_game_pks_to_poll.assert_called_once_with(
        start_date_str=start_date.strftime('%Y-%m-%d'),
        end_date_str=end_date.strftime('%Y-%m-%d')
    )

    # Verify processing for each game
    assert mock_get_game_feed.call_count == len(past_game_pks)
    assert mock_upload_to_s3.call_count == len(past_game_pks)
    assert mock_send_to_kafka.call_count == len(past_game_pks)
    assert mock_sleep.call_count == len(past_game_pks) # Called after each game

    # Verify logging and flush
    mock_logger.info.assert_any_call(f"Starting backfill for the last {backfill_days} days...")
    mock_logger.info.assert_any_call(f"Fetching feed data for {len(past_game_pks)} games...")
    mock_logger.info.assert_any_call(f"Backfill complete. Processed feeds for {len(past_game_pks)} games.")
    mock_producer.flush.assert_called_once()


@patch('poller.get_game_pks_to_poll', return_value=set()) # No games found
@patch('poller.get_game_feed')
@patch('poller.send_to_kafka')
@patch('poller.upload_to_s3')
@patch('poller.logger')
def test_run_backfill_no_games_found(
    mock_logger, mock_upload_to_s3, mock_send_to_kafka,
    mock_get_game_feed, mock_get_game_pks_to_poll
):
    """Test backfill when no games are found in the date range."""
    mock_producer = MagicMock()
    mock_s3_client = MagicMock()
    backfill_days = 1

    run_backfill(backfill_days, mock_producer, mock_s3_client)

    mock_get_game_pks_to_poll.assert_called_once()
    mock_get_game_feed.assert_not_called()
    mock_upload_to_s3.assert_not_called()
    mock_send_to_kafka.assert_not_called()
    mock_logger.info.assert_any_call("No games found in the specified date range.")
    mock_producer.flush.assert_not_called() # Flush is NOT called within run_backfill if no games
