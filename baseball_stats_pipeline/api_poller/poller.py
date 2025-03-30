import requests
import json
import time
import os
import logging
import argparse # Added
import boto3
from botocore.exceptions import ClientError
from kafka import KafkaProducer
from kafka.errors import KafkaError
from datetime import datetime, timedelta # Added timedelta

# --- Configuration ---
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'kafka:9092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'live_game_data')
SCHEDULE_POLL_INTERVAL_SECONDS = int(os.environ.get('SCHEDULE_POLL_INTERVAL_SECONDS', 60 * 5))
GAME_POLL_INTERVAL_SECONDS = int(os.environ.get('GAME_POLL_INTERVAL_SECONDS', 30))
MLB_API_BASE_URL = "https://statsapi.mlb.com/api/v1"

# S3/MinIO Configuration
S3_ENDPOINT_URL = os.environ.get('S3_ENDPOINT_URL')
S3_ACCESS_KEY = os.environ.get('S3_ACCESS_KEY')
S3_SECRET_KEY = os.environ.get('S3_SECRET_KEY')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', 'baseball-stats-raw')
S3_REGION = os.environ.get('S3_REGION', 'us-east-1')

# --- Logging Setup ---
logger = logging.getLogger('api_poller')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# --- Argument Parser Setup ---
parser = argparse.ArgumentParser(description="Poll MLB Stats API for game data.")
parser.add_argument(
    "--backfill-days",
    type=int,
    default=0,
    help="Number of past days to fetch data for (e.g., 1 for yesterday and today). Runs once and exits if > 0."
)
# Potentially add --start-date and --end-date later if more control is needed

# --- Kafka Producer Setup ---
def create_kafka_producer(retries=5, delay=2): # Reduced delay
    """Creates and returns a KafkaProducer instance with retry logic."""
    attempt = 0
    while attempt < retries:
        attempt += 1
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3,
                request_timeout_ms=30000
            )
            logger.info(f"Successfully connected to Kafka broker at {KAFKA_BROKER}")
            return producer
        except KafkaError as e:
            logger.error(f"Attempt {attempt}/{retries}: Kafka connection error: {e}. Retrying in {delay} seconds...")
            time.sleep(delay)
        except Exception as e:
            logger.error(f"Attempt {attempt}/{retries}: Unexpected error connecting to Kafka: {e}. Retrying in {delay} seconds...")
            time.sleep(delay)
    logger.critical(f"Failed to connect to Kafka after {retries} attempts.")
    return None

# --- S3 Client Setup ---
def create_s3_client(retries=5, delay=2): # Reduced delay
    """Creates and returns a boto3 S3 client instance with retry logic."""
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
            # Test connection by listing buckets
            s3_client.list_buckets()
            logger.info(f"Successfully connected to S3 endpoint: {s3_endpoint_url or 'AWS S3 default'}")

            # Ensure the target bucket exists, creating if necessary
            try:
                s3_client.head_bucket(Bucket=s3_bucket_name)
                logger.info(f"S3 bucket '{s3_bucket_name}' exists.")
            except ClientError as head_error:
                error_code = head_error.response.get("Error", {}).get("Code")
                if error_code == '404' or error_code == 'NoSuchBucket':
                    logger.warning(f"S3 bucket '{s3_bucket_name}' not found. Attempting to create...")
                    try:
                        # Adjust create_bucket call based on region constraints if using AWS S3
                        # For MinIO, no location constraint is needed typically
                        if s3_endpoint_url and 'amazonaws' not in s3_endpoint_url: # Likely MinIO or other S3 compatible
                             s3_client.create_bucket(Bucket=s3_bucket_name)
                        elif s3_region == 'us-east-1': # AWS specific region handling
                            s3_client.create_bucket(Bucket=s3_bucket_name)
                        else:
                            s3_client.create_bucket(Bucket=s3_bucket_name, CreateBucketConfiguration={'LocationConstraint': s3_region})
                        logger.info(f"S3 bucket '{s3_bucket_name}' created successfully.")
                        # Optional: Add a short wait after bucket creation for consistency
                        time.sleep(2)
                    except ClientError as create_error:
                        logger.error(f"Failed to create S3 bucket '{s3_bucket_name}': {create_error}")
                        raise create_error # Raise error to trigger retry or failure
                else:
                    # Re-raise other head_bucket errors (like permissions)
                    raise head_error
            # If connection and bucket check/creation succeeded, return the client
            return s3_client

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            if error_code == 'InvalidAccessKeyId' or error_code == 'SignatureDoesNotMatch':
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
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout as e:
            logger.warning(f"Attempt {attempt}/{retries}: Timeout fetching {description} from {url}: {e}")
        except requests.exceptions.HTTPError as e:
            logger.error(f"Attempt {attempt}/{retries}: HTTP error fetching {description} from {url}: {e.response.status_code} {e.response.reason}")
            if 400 <= e.response.status_code < 500:
                 logger.error(f"Client error {e.response.status_code}, not retrying.")
                 break
        except requests.exceptions.RequestException as e:
            logger.error(f"Attempt {attempt}/{retries}: Request error fetching {description} from {url}: {e}")
        except json.JSONDecodeError as e:
             logger.error(f"Attempt {attempt}/{retries}: Error decoding JSON from {description} at {url}: {e}")
             break
        if attempt < retries:
            logger.info(f"Retrying in {delay} seconds...")
            time.sleep(delay)
    logger.error(f"Failed to fetch {description} from {url} after {retries} attempts.")
    return None

def get_game_pks_to_poll(start_date_str=None, end_date_str=None):
    """
    Fetches the schedule and returns a set of gamePks to poll.
    If start/end dates are provided, fetches all games in that range.
    Otherwise, fetches today's schedule and returns only live games.
    """
    game_pks = set()
    base_url = f"{MLB_API_BASE_URL}/schedule?sportId=1&gameType=R&gameType=F&gameType=D&gameType=L&gameType=W&gameType=A"

    if start_date_str and end_date_str:
        # Backfill mode: Fetch all games in the date range
        schedule_url = f"{base_url}&startDate={start_date_str}&endDate={end_date_str}"
        description = f"schedule from {start_date_str} to {end_date_str}"
        logger.info(f"Fetching {description}...")
        schedule_data = fetch_with_retry(schedule_url, description, timeout=20) # Longer timeout for potentially larger response
        if schedule_data and schedule_data.get("totalGames", 0) > 0:
            for date_info in schedule_data.get("dates", []):
                for game in date_info.get("games", []):
                    game_pk = game.get("gamePk")
                    if game_pk:
                        game_pks.add(game_pk)
        logger.info(f"Found {len(game_pks)} games in date range {start_date_str} to {end_date_str}.")
    else:
        # Live mode: Fetch today's schedule for live games
        schedule_url = base_url
        description = "today's schedule"
        logger.info("Checking schedule for live games...")
        schedule_data = fetch_with_retry(schedule_url, description, timeout=10)
        if schedule_data and schedule_data.get("totalGames", 0) > 0:
            for date_info in schedule_data.get("dates", []):
                for game in date_info.get("games", []):
                    game_status = game.get("status", {}).get("abstractGameState", "")
                    game_pk = game.get("gamePk")
                    if game_status == "Live" and game_pk:
                        game_pks.add(game_pk)
        logger.info(f"Found live games: {game_pks if game_pks else 'None'}")

    return game_pks

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
        game_data = data.get("gameData", {})
        game_info = game_data.get("game", {})
        status_info = game_data.get("status", {})
        s3_bucket_name = os.environ.get('S3_BUCKET_NAME', 'baseball-stats-raw')

        season = game_info.get("season", "unknown_season")
        game_type_code = game_info.get("type", "U")
        status = status_info.get("abstractGameState", "Unknown").lower()

        game_type_map = {
            'R': 'regular', 'F': 'wildcard', 'D': 'division',
            'L': 'league', 'W': 'worldseries', 'S': 'spring', 'A': 'allstar'
        }
        game_type = game_type_map.get(game_type_code, f"unknown_type_{game_type_code}")

        timestamp = data.get("metaData", {}).get("timeStamp", datetime.utcnow().strftime("%Y%m%d_%H%M%S%f"))
        safe_timestamp = "".join(c for c in timestamp if c.isalnum() or c in ('_', '-'))

        s3_key = f"{season}/{game_type}/{status}/{game_pk}/{safe_timestamp}.json"
        json_body = json.dumps(data, ensure_ascii=False, indent=2)

        s3_client.put_object(
            Bucket=s3_bucket_name,
            Key=s3_key,
            Body=json_body.encode('utf-8'),
            ContentType='application/json'
        )
        logger.debug(f"Uploaded raw data for game {game_pk} to s3://{s3_bucket_name}/{s3_key}")
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
        return False
    if not data or not game_pk:
        logger.warning("Attempted to send empty data or missing game_pk to Kafka.")
        return False
    try:
        key = str(game_pk).encode('utf-8')
        future = producer.send(KAFKA_TOPIC, key=key, value=data)
        return True
    except KafkaError as e:
        logger.error(f"Kafka error sending data for game {game_pk}: {e}")
    except BufferError:
         logger.error(f"Kafka producer buffer is full for game {game_pk}. Check producer/broker performance.")
         producer.flush(timeout=5)
    except Exception as e:
        logger.error(f"Unexpected error sending data to Kafka for game {game_pk}: {e}")
    return False

# --- Backfill Function ---
def run_backfill(days_ago: int, kafka_producer: KafkaProducer, s3_client):
    """Fetches data for games within the specified past days, runs once, and exits."""
    logger.info(f"Starting backfill for the last {days_ago} days...")
    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=days_ago)
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    game_pks_to_fetch = get_game_pks_to_poll(start_date_str=start_date_str, end_date_str=end_date_str)

    if not game_pks_to_fetch:
        logger.info("No games found in the specified date range.")
        return

    processed_count = 0
    kafka_failures = 0
    s3_failures = 0

    logger.info(f"Fetching feed data for {len(game_pks_to_fetch)} games...")
    for game_pk in game_pks_to_fetch:
        game_data = get_game_feed(game_pk)
        if game_data:
            processed_count += 1
            upload_success = upload_to_s3(s3_client, game_data, game_pk)
            if upload_success:
                if not send_to_kafka(kafka_producer, game_data, game_pk):
                    kafka_failures += 1
            else:
                s3_failures +=1
                logger.warning(f"Skipping Kafka send for game {game_pk} due to S3 upload failure.")
        else:
            logger.warning(f"No feed data received for game {game_pk} after retries.")
        # Optional: Add a small delay between game fetches to avoid overwhelming the API
        time.sleep(0.5)

    logger.info(f"Backfill complete. Processed feeds for {processed_count} games.")
    logger.info(f"S3 Upload Failures: {s3_failures}")
    logger.info(f"Kafka Send Failures: {kafka_failures}")

    if kafka_producer:
        try:
            logger.info("Flushing Kafka producer...")
            kafka_producer.flush(timeout=30) # Longer timeout for potentially more messages
            logger.info("Kafka producer flushed.")
        except Exception as e:
            logger.error(f"Error flushing Kafka producer during backfill: {e}")


# --- Live Polling Function ---
def run_live_polling(kafka_producer: KafkaProducer, s3_client):
    """Runs the continuous polling loop for live games."""
    logger.info("Starting live polling loop...")
    currently_polling = set()
    last_schedule_check_time = 0

    try:
        while True:
            now = time.time()
            # --- Schedule Check ---
            if now - last_schedule_check_time >= SCHEDULE_POLL_INTERVAL_SECONDS:
                try:
                    # Use the renamed function without date args for live games
                    live_pks = get_game_pks_to_poll()
                    last_schedule_check_time = now
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

            # --- Game Polling ---
            if not currently_polling:
                sleep_duration = max(10, SCHEDULE_POLL_INTERVAL_SECONDS // 2)
                logger.info(f"No live games currently identified. Sleeping for {sleep_duration} seconds...")
                time.sleep(sleep_duration)
                continue

            logger.info(f"Polling {len(currently_polling)} identified live games: {currently_polling}")
            poll_start_time = time.time()
            processed_count = 0
            kafka_failures = 0

            for game_pk in list(currently_polling): # Iterate over a copy
                game_data = get_game_feed(game_pk)
                if game_data:
                    processed_count += 1
                    game_status = game_data.get("gameData", {}).get("status", {}).get("abstractGameState", "")

                    # Always upload and send, even if status changed since schedule check
                    # (ensures final state is captured)
                    upload_success = upload_to_s3(s3_client, game_data, game_pk)
                    if upload_success:
                        if not send_to_kafka(kafka_producer, game_data, game_pk):
                            kafka_failures += 1
                    else:
                        logger.warning(f"Skipping Kafka send for game {game_pk} due to S3 upload failure.")

                    # If game is no longer live according to its feed, remove from polling set
                    if game_status != "Live":
                        logger.info(f"Game {game_pk} status in feed is now '{game_status}'. Removing from active polling.")
                        currently_polling.discard(game_pk)

                else:
                    logger.warning(f"No data received for game {game_pk} after retries. Keeping in polling list for now.")

            poll_duration = time.time() - poll_start_time
            logger.info(f"Finished polling cycle for {processed_count} games in {poll_duration:.2f} seconds. Kafka failures: {kafka_failures}.")

            # --- Flush and Sleep ---
            if kafka_producer:
                try:
                    kafka_producer.flush(timeout=10)
                    logger.debug("Kafka producer flushed.")
                except Exception as e:
                    logger.error(f"Error flushing Kafka producer: {e}")

            sleep_time = max(1, GAME_POLL_INTERVAL_SECONDS - poll_duration)
            logger.info(f"Sleeping for {sleep_time:.2f} seconds...")
            time.sleep(sleep_time)

    except KeyboardInterrupt:
        logger.info("Shutdown signal received.")
    except Exception as e:
        logger.exception(f"Unhandled exception in main loop: {e}")


# --- Main Execution ---
def main():
    """Sets up connections and runs either backfill or live polling based on args."""
    args = parser.parse_args()

    logger.info("Initializing API Poller...")
    kafka_producer = create_kafka_producer()
    s3_client = create_s3_client()

    if not kafka_producer or not s3_client:
        logger.critical("Exiting due to Kafka or S3 connection failure.")
        # No need to close producer here, finally block handles it
        return

    try:
        if args.backfill_days > 0:
            # Run backfill mode
            run_backfill(args.backfill_days, kafka_producer, s3_client)
        else:
            # Run live polling mode
            run_live_polling(kafka_producer, s3_client)

    finally:
        logger.info("Shutting down API Poller...")
        if kafka_producer:
            logger.info("Closing Kafka producer...")
            try:
                kafka_producer.close(timeout=10)
            except Exception as e:
                logger.error(f"Error closing Kafka producer: {e}")
        logger.info("API Poller stopped.")

if __name__ == "__main__":
    main()
