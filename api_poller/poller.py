import requests
import json
import time
import os
import logging
from kafka import KafkaProducer
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
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Kafka Producer Setup ---
def create_kafka_producer():
    """Creates and returns a KafkaProducer instance."""
    producer = None
    retries = 5
    while retries > 0 and producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all', # Ensure messages are received by all replicas
                retries=3 # Retry sending messages on failure
            )
            logging.info(f"Successfully connected to Kafka broker at {KAFKA_BROKER}")
        except Exception as e:
            logging.error(f"Failed to connect to Kafka: {e}. Retrying in 10 seconds...")
            retries -= 1
            time.sleep(10)
    return producer

# --- API Interaction ---
def get_live_game_pks():
    """Fetches the schedule and returns a list of gamePks for live games."""
    live_game_pks = []
    schedule_url = f"{MLB_API_BASE_URL}/schedule?sportId=1&gameType=R&gameType=F&gameType=D&gameType=L&gameType=W&gameType=A" # Regular season + Postseason types
    try:
        response = requests.get(schedule_url, timeout=10)
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        schedule_data = response.json()

        if schedule_data.get("totalGames", 0) > 0:
            for date_info in schedule_data.get("dates", []):
                for game in date_info.get("games", []):
                    game_status = game.get("status", {}).get("abstractGameState", "")
                    if game_status == "Live":
                        live_game_pks.append(game.get("gamePk"))
        logging.info(f"Found live games: {live_game_pks}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching schedule: {e}")
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding schedule JSON: {e}")
    return live_game_pks

def get_game_feed(game_pk):
    """Fetches the live feed data for a specific gamePk."""
    feed_url = f"https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live" # v1.1 for live feed
    try:
        response = requests.get(feed_url, timeout=15)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching game feed for {game_pk}: {e}")
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding game feed JSON for {game_pk}: {e}")
    return None

# --- Data Handling ---
def save_raw_json(data, game_pk):
    """Saves the raw JSON data to the designated path."""
    try:
        timestamp = data.get("metaData", {}).get("timeStamp", datetime.utcnow().strftime("%Y%m%d_%H%M%S%f"))
        # Ensure timestamp is filesystem-safe
        safe_timestamp = "".join(c for c in timestamp if c.isalnum() or c in ('_', '-'))
        game_path = os.path.join(RAW_DATA_PATH, str(game_pk))
        os.makedirs(game_path, exist_ok=True)
        file_path = os.path.join(game_path, f"{safe_timestamp}.json")
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        # logging.info(f"Saved raw data for game {game_pk} to {file_path}") # Can be verbose
    except Exception as e:
        logging.error(f"Error saving raw JSON for game {game_pk}: {e}")

def send_to_kafka(producer, data, game_pk):
    """Sends data to the Kafka topic."""
    if producer:
        try:
            # Use gamePk as the key for partitioning (optional but good practice)
            key = str(game_pk).encode('utf-8')
            future = producer.send(KAFKA_TOPIC, key=key, value=data)
            # Wait for confirmation (optional, adds latency but ensures delivery)
            # record_metadata = future.get(timeout=10)
            # logging.info(f"Sent data for game {game_pk} to Kafka topic {record_metadata.topic} partition {record_metadata.partition}")
        except Exception as e:
            logging.error(f"Error sending data to Kafka for game {game_pk}: {e}")
            # Consider attempting to reconnect producer if send fails repeatedly
    else:
        logging.warning("Kafka producer not available. Cannot send message.")


# --- Main Polling Loop ---
def main():
    """Main function to run the polling loop."""
    logging.info("Starting API Poller...")
    kafka_producer = create_kafka_producer()
    if not kafka_producer:
        logging.critical("Failed to create Kafka producer after retries. Exiting.")
        return

    currently_polling = set()
    last_schedule_check = 0

    while True:
        now = time.time()

        # Check for live games periodically
        if now - last_schedule_check >= SCHEDULE_POLL_INTERVAL_SECONDS:
            logging.info("Checking schedule for live games...")
            live_pks = set(get_live_game_pks())
            last_schedule_check = now

            # Update the set of games to poll
            games_to_stop = currently_polling - live_pks
            games_to_start = live_pks - currently_polling

            if games_to_stop:
                logging.info(f"Stopping polling for finished games: {games_to_stop}")
                currently_polling.difference_update(games_to_stop)
            if games_to_start:
                logging.info(f"Starting polling for new live games: {games_to_start}")
                currently_polling.update(games_to_start)

        # Poll active games
        if not currently_polling:
            logging.info(f"No live games to poll. Sleeping for {SCHEDULE_POLL_INTERVAL_SECONDS} seconds...")
            time.sleep(SCHEDULE_POLL_INTERVAL_SECONDS) # Sleep longer if no games are live
            continue

        logging.info(f"Polling {len(currently_polling)} live games: {currently_polling}")
        for game_pk in list(currently_polling): # Iterate over a copy
            game_data = get_game_feed(game_pk)
            if game_data:
                # Check if game is actually still live in the feed data itself
                game_status = game_data.get("gameData", {}).get("status", {}).get("abstractGameState", "")
                if game_status != "Live":
                    logging.info(f"Game {game_pk} status changed to {game_status}. Removing from polling list.")
                    currently_polling.discard(game_pk)
                    # Optionally fetch one last time if 'Final' to ensure final state is captured?
                else:
                    save_raw_json(game_data, game_pk)
                    send_to_kafka(kafka_producer, game_data, game_pk)
            else:
                # Consider removing game from polling if feed fails multiple times?
                logging.warning(f"No data received for game {game_pk}. Will retry.")

            time.sleep(0.1) # Small delay between polling individual games

        # Flush Kafka producer buffer periodically
        if kafka_producer:
            kafka_producer.flush()

        logging.info(f"Finished polling cycle. Sleeping for {GAME_POLL_INTERVAL_SECONDS} seconds...")
        time.sleep(GAME_POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
