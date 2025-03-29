import requests
import json
import requests
import requests
import json
import time
import os
import logging
import boto3
from botocore.exceptions import ClientError
from kafka import KafkaProducer
from kafka.errors import KafkaError # Import specific Kafka errors
from datetime import datetime

# --- Configuration ---
# Use environment variables for flexibility, provide defaults
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'kafka:9092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'live_game_data')
# RAW_DATA_PATH = os.environ.get('RAW_DATA_PATH', '/data/raw_json') # No longer saving locally
SCHEDULE_POLL_INTERVAL_SECONDS = int(os.environ.get('SCHEDULE_POLL_INTERVAL_SECONDS', 60 * 5))
GAME_POLL_INTERVAL_SECONDS = int(os.environ.get('GAME_POLL_INTERVAL_SECONDS', 30))
MLB_API_BASE_URL = "https://statsapi.mlb.com/api/v1"

# S3/MinIO Configuration
S3_ENDPOINT_URL = os.environ.get('S3_ENDPOINT_URL') # Required for MinIO, optional for AWS S3
S3_ACCESS_KEY = os.environ.get('S3_ACCESS_KEY')
S3_SECRET_KEY = os.environ.get('S3_SECRET_KEY')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', 'baseball-stats-raw')
S3_REGION = os.environ.get('S3_REGION', 'us-east-1') # Default region if needed

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

# --- S3 Client Setup ---
def create_s3_client(retries=5, delay=10):
    """Creates and returns a boto3 S3 client instance with retry logic."""
    # Read environment variables inside the function to allow patching during tests
    s3_endpoint_url = os.environ.get('S3_ENDPOINT_URL')
    s3_access_key = os.environ.get('S3_ACCESS_KEY')
    s3_secret_key = os.environ.get('S3_SECRET_KEY')
    s3_bucket_name = os.environ.get('S3_BUCKET_NAME', 'baseball-stats-raw')
    s3_region = os.environ.get('S3_REGION', 'us-east-1')

    attempt = 0
    while attempt < retries:
        attempt += 1
        try:
            s3_client = boto3.client(
                's3',
                endpoint_url=s3_endpoint_url,
                aws_access_key_id=s3_access_key,
                aws_secret_access_key=s3_secret_key,
                region_name=s3_region
            )
            # Test connection by performing a head_bucket call on the target bucket
            s3_client.head_bucket(Bucket=s3_bucket_name)
            logger.info(f"Successfully connected to S3 endpoint: {s3_endpoint_url or 'AWS S3 default'}")
            return s3_client
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            if error_code == 'NoSuchBucket':
                 logger.error(f"Attempt {attempt}/{retries}: S3 bucket '{s3_bucket_name}' not found. Please create it.")
                 # Optionally try to create the bucket here if desired and permissions allow
                 # try:
                 #     s3_client.create_bucket(Bucket=S3_BUCKET_NAME)
                 #     logger.info(f"Bucket '{S3_BUCKET_NAME}' created.")
                 #     return s3_client # Return client after creation
                 # except ClientError as create_e:
                 #     logger.error(f"Failed to create bucket '{S3_BUCKET_NAME}': {create_e}")
                 #     break # Stop retrying if bucket creation fails
            elif error_code == 'InvalidAccessKeyId' or error_code == 'SignatureDoesNotMatch':
                 logger.error(f"Attempt {attempt}/{retries}: S3 authentication failed (Invalid Key/Secret). Retrying in {delay} seconds...")
                 time.sleep(delay)
            else:
                 logger.error(f"Attempt {attempt}/{retries}: S3 connection error: {e}. Retrying in {delay} seconds...")
                 time.sleep(delay)
        except Exception as e:
            logger.error(f"Attempt {attempt}/{retries}: Unexpected error connecting to S3: {e}. Retrying in {delay} seconds...")
            time.sleep(delay)
    logger.critical(f"Failed to connect to S3 after {retries} attempts.")
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
def upload_to_s3(s3_client, data, game_pk):
    """Uploads the raw JSON data to S3/MinIO."""
    if not s3_client:
        logger.warning("S3 client not available. Cannot upload message.")
        return False
    if not data or not game_pk:
        logger.warning("Attempted to upload empty data or missing game_pk.")
        return False

    try:
        # Extract necessary fields for path construction
        game_data = data.get("gameData", {})
        game_info = game_data.get("game", {})
        status_info = game_data.get("status", {})

        season = game_info.get("season", "unknown_season")
        game_type_code = game_info.get("type", "U") # U for unknown/unspecified
        status = status_info.get("abstractGameState", "Unknown").lower() # e.g., live, final, preview

        # Map game type code to readable name
        game_type_map = {
            'R': 'regular',
            'F': 'wildcard', # Assuming F is Wild Card
            'D': 'division', # Assuming D is Division Series
            'L': 'league',   # Assuming L is League Championship Series
            'W': 'worldseries',
            'S': 'spring',   # Spring Training
            'A': 'allstar',  # All-Star Game
            # Add other types as needed
        }
        game_type = game_type_map.get(game_type_code, f"unknown_type_{game_type_code}")

        # Use game timestamp if available, otherwise use current time
        timestamp = data.get("metaData", {}).get("timeStamp", datetime.utcnow().strftime("%Y%m%d_%H%M%S%f"))
        safe_timestamp = "".join(c for c in timestamp if c.isalnum() or c in ('_', '-'))

        # Construct S3 key
        s3_key = f"{season}/{game_type}/{status}/{game_pk}/{safe_timestamp}.json"

        # Serialize data to JSON string
        json_body = json.dumps(data, ensure_ascii=False, indent=2)

        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=json_body.encode('utf-8'), # Encode string to bytes
            ContentType='application/json'
        )
        logger.debug(f"Uploaded raw data for game {game_pk} to s3://{S3_BUCKET_NAME}/{s3_key}")
        return True
    except ClientError as e:
        logger.error(f"S3 error uploading data for game {game_pk}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error uploading data to S3 for game {game_pk}: {e}")
    return False


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
    s3_client = create_s3_client()

    # Exit if Kafka or S3 connection failed permanently
    if not kafka_producer or not s3_client:
        logger.critical("Exiting due to Kafka or S3 connection failure.")
        # Clean up any successful connection
        if kafka_producer:
            kafka_producer.close()
        # No explicit close needed for boto3 client typically
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
                        # Still upload the final state to S3
                        upload_to_s3(s3_client, game_data, game_pk)
                        # Optionally send final state to Kafka too? Depends on requirements.
                        # send_to_kafka(kafka_producer, game_data, game_pk)
                    else:
                        # Upload to S3 first
                        upload_success = upload_to_s3(s3_client, game_data, game_pk)
                        # Then attempt to send to Kafka (only if upload succeeded?)
                        if upload_success:
                            if not send_to_kafka(kafka_producer, game_data, game_pk):
                                kafka_failures += 1
                                # Decide on action if Kafka fails repeatedly (e.g., stop polling game?)
                        else:
                            logger.warning(f"Skipping Kafka send for game {game_pk} due to S3 upload failure.")
                else:
                    logger.warning(f"No data received for game {game_pk} after retries. Keeping in polling list for now.")
                    # Consider adding logic to remove game if it fails consistently

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
        logger.info("Shutting down API Poller...")
        if kafka_producer:
            logger.info("Closing Kafka producer...")
            try:
                kafka_producer.close(timeout=10)
            except Exception as e:
                logger.error(f"Error closing Kafka producer: {e}")
        # No explicit close needed for boto3 client typically
        logger.info("API Poller stopped.")


if __name__ == "__main__":
    main()
