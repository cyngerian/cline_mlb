import requests
import json
import requests
import json
import time
import os
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError # Import specific Kafka errors
from datetime import datetime

# --- Configuration ---
# Use environment variables for flexibility, provide defaults
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'kafka:9092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'live_game_data')
RAW_DATA_PATH = os.environ.get('RAW_DATA_PATH', '/data/raw_json') # Mounted volume path inside container
SCHEDULE_POLL_INTERVAL_SECONDS = 60 * 5 # Check for new live games every 5 minutes
GAME_POLL_INTERVAL_SECONDS = 30 # Poll live games every 30 seconds
MLB_API_BASE_URL = "https://statsapi.mlb.com/api/v1"

# --- Logging Setup ---
# Use specific logger name
logger = logging.getLogger('api_poller')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


# --- Kafka Producer Setup ---
def create_kafka_producer(retries=5, delay=10):
    """Creates and returns a KafkaProducer instance."""
    """Creates and returns a KafkaProducer instance with retry logic."""
    attempt = 0
    while attempt < retries:
        attempt += 1
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all', # Ensure messages are received by all replicas
                retries=3, # Kafka client internal retries for sending
                request_timeout_ms=30000 # Increase timeout for broker connection
            )
            logger.info(f"Successfully connected to Kafka broker at {KAFKA_BROKER}")
            return producer # Return producer on success
        except KafkaError as e:
            logger.error(f"Attempt {attempt}/{retries}: Kafka connection error: {e}. Retrying in {delay} seconds...")
            time.sleep(delay)
        except Exception as e:
            logger.error(f"Attempt {attempt}/{retries}: Unexpected error connecting to Kafka: {e}. Retrying in {delay} seconds...")
            time.sleep(delay)
    logger.critical(f"Failed to connect to Kafka after {retries} attempts.")
    return None

# --- API Interaction with Retries ---
def fetch_with_retry(url, description, retries=3, delay=5, timeout=15):
    """Fetches data from a URL with retry logic."""
    attempt = 0
    while attempt < retries:
        attempt += 1
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
            return response.json()
        except requests.exceptions.Timeout as e:
            logger.warning(f"Attempt {attempt}/{retries}: Timeout fetching {description} from {url}: {e}")
        except requests.exceptions.HTTPError as e:
            logger.error(f"Attempt {attempt}/{retries}: HTTP error fetching {description} from {url}: {e.response.status_code} {e.response.reason}")
            # Don't retry on client errors (4xx) unless specific ones are allowed
            if 400 <= e.response.status_code < 500:
                 logger.error(f"Client error {e.response.status_code}, not retrying.")
                 break
        except requests.exceptions.RequestException as e:
            logger.error(f"Attempt {attempt}/{retries}: Request error fetching {description} from {url}: {e}")
        except json.JSONDecodeError as e:
             logger.error(f"Attempt {attempt}/{retries}: Error decoding JSON from {description} at {url}: {e}")
             # Don't retry if JSON is invalid, likely a persistent issue
             break

        if attempt < retries:
            logger.info(f"Retrying in {delay} seconds...")
            time.sleep(delay)

    logger.error(f"Failed to fetch {description} from {url} after {retries} attempts.")
    return None


def get_live_game_pks():
    """Fetches the schedule and returns a set of gamePks for live games."""
    live_game_pks = set()
    schedule_url = f"{MLB_API_BASE_URL}/schedule?sportId=1&gameType=R&gameType=F&gameType=D&gameType=L&gameType=W&gameType=A"
    schedule_data = fetch_with_retry(schedule_url, "schedule", timeout=10)

    if schedule_data and schedule_data.get("totalGames", 0) > 0:
        for date_info in schedule_data.get("dates", []):
            for game in date_info.get("games", []):
                game_status = game.get("status", {}).get("abstractGameState", "")
                game_pk = game.get("gamePk")
                if game_status == "Live" and game_pk:
                    live_game_pks.add(game_pk)
    logger.info(f"Found live games: {live_game_pks if live_game_pks else 'None'}")
    return live_game_pks

def get_game_feed(game_pk):
    """Fetches the live feed data for a specific gamePk using retry logic."""
    feed_url = f"https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live"
    return fetch_with_retry(feed_url, f"game feed for {game_pk}")

# --- Data Handling ---
def save_raw_json(data, game_pk):
    """Saves the raw JSON data to the designated path."""
    if not data or not game_pk:
        logger.warning("Attempted to save empty data or missing game_pk.")
        return
    try:
        # Use game timestamp if available, otherwise use current time
        timestamp = data.get("metaData", {}).get("timeStamp", datetime.utcnow().strftime("%Y%m%d_%H%M%S%f"))
        # Ensure timestamp is filesystem-safe
        safe_timestamp = "".join(c for c in timestamp if c.isalnum() or c in ('_', '-'))
        game_path = os.path.join(RAW_DATA_PATH, str(game_pk))
        os.makedirs(game_path, exist_ok=True) # Ensure directory exists
        file_path = os.path.join(game_path, f"{safe_timestamp}.json")
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2) # Use indent=2 for smaller files
        # logger.debug(f"Saved raw data for game {game_pk} to {file_path}") # Use debug level
    except OSError as e:
        # Modify log message to not depend on file_path if makedirs failed
        logger.error(f"OS error saving raw JSON for game {game_pk} (path: {game_path}): {e}")
    except Exception as e:
        logger.error(f"Unexpected error saving raw JSON for game {game_pk}: {e}")

def send_to_kafka(producer: KafkaProducer, data, game_pk):
    """Sends data to the Kafka topic with error handling."""
    if not producer:
        logger.warning("Kafka producer not available. Cannot send message.")
        return False # Indicate failure
    if not data or not game_pk:
        logger.warning("Attempted to send empty data or missing game_pk to Kafka.")
        return False

    try:
        key = str(game_pk).encode('utf-8')
        future = producer.send(KAFKA_TOPIC, key=key, value=data)
        # Optional: Block for acknowledgement (adds latency)
        # record_metadata = future.get(timeout=10)
        # logger.debug(f"Sent data for game {game_pk} to Kafka topic {record_metadata.topic} partition {record_metadata.partition}")
        return True # Indicate success
    except KafkaError as e:
        logger.error(f"Kafka error sending data for game {game_pk}: {e}")
        # Consider more specific handling based on error type (e.g., retries, producer recreation)
    except BufferError:
         logger.error(f"Kafka producer buffer is full for game {game_pk}. Check producer/broker performance.")
         # Consider flushing or waiting
         producer.flush(timeout=5) # Attempt to flush with timeout
    except Exception as e:
        logger.error(f"Unexpected error sending data to Kafka for game {game_pk}: {e}")

    return False # Indicate failure


# --- Main Polling Loop ---
def main():
    """Main function to run the polling loop."""
    logger.info("Starting API Poller...")
    kafka_producer = create_kafka_producer()

    # Exit if Kafka connection failed permanently
    if not kafka_producer:
        logger.critical("Exiting due to Kafka connection failure.")
        return

    currently_polling = set() # Set of gamePks being actively polled
    last_schedule_check_time = 0

    try: # Wrap main loop in try/except for graceful shutdown
        while True:
            now = time.time()

            # --- Check Schedule ---
            if now - last_schedule_check_time >= SCHEDULE_POLL_INTERVAL_SECONDS:
                logger.info("Checking schedule for live games...")
                try:
                    live_pks = get_live_game_pks() # Returns a set
                    last_schedule_check_time = now

                    # Update the set of games being polled
                    games_to_stop = currently_polling - live_pks
                    games_to_start = live_pks - currently_polling

                    if games_to_stop:
                        logger.info(f"Stopping polling for finished/non-live games: {games_to_stop}")
                        currently_polling.difference_update(games_to_stop)
                    if games_to_start:
                        logger.info(f"Starting polling for new live games: {games_to_start}")
                        currently_polling.update(games_to_start)
                except Exception as e:
                    logger.error(f"Error during schedule check: {e}. Will retry later.")
                    # Avoid updating last_schedule_check_time on error to force retry sooner

            # --- Poll Active Games ---
            if not currently_polling:
                sleep_duration = max(10, SCHEDULE_POLL_INTERVAL_SECONDS // 2) # Sleep shorter if no games, but not too short
                logger.info(f"No live games currently identified. Sleeping for {sleep_duration} seconds...")
                time.sleep(sleep_duration)
                continue # Skip to next schedule check

            logger.info(f"Polling {len(currently_polling)} identified live games: {currently_polling}")
            poll_start_time = time.time()
            processed_count = 0
            kafka_failures = 0

            for game_pk in list(currently_polling): # Iterate over a copy in case set changes
                game_data = get_game_feed(game_pk)
                if game_data:
                    processed_count += 1
                    # Double-check status from the feed itself
                    game_status = game_data.get("gameData", {}).get("status", {}).get("abstractGameState", "")
                    if game_status != "Live":
                        logger.info(f"Game {game_pk} status in feed is now '{game_status}'. Removing from active polling.")
                        currently_polling.discard(game_pk)
                        # Optionally fetch one last time if 'Final' to ensure final state is captured?
                        # Consider sending a specific "game ended" message to Kafka?
                    else:
                        # Save locally first
                        save_raw_json(game_data, game_pk)
                        # Then attempt to send to Kafka
                        if not send_to_kafka(kafka_producer, game_data, game_pk):
                            kafka_failures += 1
                            # Decide on action if Kafka fails repeatedly (e.g., stop polling game?)
                else:
                    logger.warning(f"No data received for game {game_pk} after retries. Keeping in polling list for now.")
                    # Consider adding logic to remove game if it fails consistently

                time.sleep(0.1) # Small delay between polling individual games to avoid overwhelming API

            # --- Post-Cycle Actions ---
            poll_duration = time.time() - poll_start_time
            logger.info(f"Finished polling cycle for {processed_count} games in {poll_duration:.2f} seconds. Kafka failures: {kafka_failures}.")

            # Flush Kafka producer buffer
            if kafka_producer:
                try:
                    kafka_producer.flush(timeout=10) # Flush remaining messages with timeout
                    logger.debug("Kafka producer flushed.")
                except Exception as e:
                    logger.error(f"Error flushing Kafka producer: {e}")

            # --- Sleep ---
            sleep_time = max(1, GAME_POLL_INTERVAL_SECONDS - poll_duration) # Ensure at least 1 sec sleep
            logger.info(f"Sleeping for {sleep_time:.2f} seconds...")
            time.sleep(sleep_time)

    except KeyboardInterrupt:
        logger.info("Shutdown signal received.")
    except Exception as e:
        logger.exception(f"Unhandled exception in main loop: {e}") # Log full traceback
    finally:
        # --- Cleanup ---
        logger.info("Shutting down API Poller...")
        if kafka_producer:
            logger.info("Closing Kafka producer...")
            kafka_producer.close(timeout=10)
        logger.info("API Poller stopped.")


if __name__ == "__main__":
    main()
