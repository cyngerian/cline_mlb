import json
import os
import logging
import time
from kafka import KafkaConsumer
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, Boolean, Date, MetaData
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.dialects.postgresql import insert as pg_insert # For UPSERT functionality
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime

# --- Configuration ---
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'kafka:9092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'live_game_data')
KAFKA_GROUP_ID = os.environ.get('KAFKA_GROUP_ID', 'rt_transformer_group')
DB_USER = os.environ.get('DB_USER', 'statsuser')
DB_PASSWORD = os.environ.get('DB_PASSWORD', 'statspassword')
DB_HOST = os.environ.get('DB_HOST', 'postgres')
DB_PORT = os.environ.get('DB_PORT', '5432')
DB_NAME = os.environ.get('DB_NAME', 'baseball_stats')
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- SQLAlchemy Setup ---
Base = declarative_base()
engine = None
SessionLocal = None

def connect_db():
    """Connects to the database and returns engine and sessionmaker."""
    global engine, SessionLocal
    retries = 5
    while retries > 0 and engine is None:
        try:
            engine = create_engine(DATABASE_URL, pool_pre_ping=True)
            # Test connection
            with engine.connect() as connection:
                logging.info("Successfully connected to the database.")
            SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
            # Create tables if they don't exist
            Base.metadata.create_all(bind=engine)
            logging.info("Database tables checked/created.")
            return engine, SessionLocal
        except SQLAlchemyError as e:
            logging.error(f"Database connection failed: {e}. Retrying in 10 seconds...")
            retries -= 1
            time.sleep(10)
    logging.critical("Could not connect to the database after multiple retries. Exiting.")
    exit(1) # Exit if DB connection fails permanently

# --- Database Models (Simplified for Real-time Focus) ---
# We'll add more complex models like PLAYS, AT_BATS, PITCHES later, likely populated by Airflow
class Game(Base):
    __tablename__ = 'games'
    game_pk = Column(Integer, primary_key=True)
    home_team_id = Column(Integer) # Add ForeignKey later if TEAMS table is robustly populated first
    away_team_id = Column(Integer) # Add ForeignKey later
    game_datetime = Column(DateTime)
    venue_name = Column(String)
    game_status = Column(String) # e.g., Preview, Live, Final
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class Team(Base):
    __tablename__ = 'teams'
    team_id = Column(Integer, primary_key=True)
    name = Column(String)
    abbreviation = Column(String)
    league = Column(String)
    division = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class Player(Base):
    __tablename__ = 'players'
    player_id = Column(Integer, primary_key=True)
    full_name = Column(String)
    primary_position = Column(String)
    birth_date = Column(Date)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class GameState(Base):
    __tablename__ = 'game_state'
    game_pk = Column(Integer, primary_key=True) # Add ForeignKey later
    inning = Column(Integer)
    top_bottom = Column(String)
    outs = Column(Integer)
    balls = Column(Integer)
    strikes = Column(Integer)
    runner_on_first_id = Column(Integer, nullable=True) # Add ForeignKey later
    runner_on_second_id = Column(Integer, nullable=True) # Add ForeignKey later
    runner_on_third_id = Column(Integer, nullable=True) # Add ForeignKey later
    current_batter_id = Column(Integer, nullable=True) # Add ForeignKey later
    current_pitcher_id = Column(Integer, nullable=True) # Add ForeignKey later
    home_score = Column(Integer)
    away_score = Column(Integer)
    last_updated = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

# --- Kafka Consumer Setup ---
def create_kafka_consumer():
    """Creates and returns a KafkaConsumer instance."""
    consumer = None
    retries = 5
    while retries > 0 and consumer is None:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                auto_offset_reset='earliest', # Start reading at the earliest message if no offset found
                enable_auto_commit=False, # Commit offsets manually after processing
                group_id=KAFKA_GROUP_ID,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=1000 # Timeout to allow checking loop condition
            )
            logging.info(f"Kafka consumer connected to {KAFKA_BROKER}, subscribed to {KAFKA_TOPIC}")
        except Exception as e:
            logging.error(f"Failed to connect Kafka consumer: {e}. Retrying in 10 seconds...")
            retries -= 1
            time.sleep(10)
    return consumer

# --- Data Processing Logic ---
def process_message(message, db_session):
    """Processes a single message from Kafka."""
    try:
        data = message.value
        game_pk = data.get("gamePk")
        game_data = data.get("gameData", {})
        live_data = data.get("liveData", {})
        metadata = data.get("metaData", {})

        if not game_pk or not game_data or not live_data:
            logging.warning(f"Incomplete data received for message offset {message.offset}. Skipping.")
            return

        # --- Upsert Game Info ---
        game_info = {
            "game_pk": game_pk,
            "home_team_id": game_data.get("teams", {}).get("home", {}).get("id"),
            "away_team_id": game_data.get("teams", {}).get("away", {}).get("id"),
            "game_datetime": game_data.get("datetime", {}).get("dateTime"), # Assuming ISO format
            "venue_name": game_data.get("venue", {}).get("name"),
            "game_status": game_data.get("status", {}).get("abstractGameState"),
            "updated_at": datetime.utcnow()
        }
        # Convert datetime string if present
        if game_info["game_datetime"]:
            try:
                game_info["game_datetime"] = datetime.fromisoformat(game_info["game_datetime"].replace('Z', '+00:00'))
            except ValueError:
                logging.warning(f"Could not parse game_datetime: {game_info['game_datetime']}")
                game_info["game_datetime"] = None # Or handle differently

        stmt = pg_insert(Game).values(game_info)
        stmt = stmt.on_conflict_do_update(
            index_elements=['game_pk'],
            set_={k: getattr(stmt.excluded, k) for k in game_info if k != 'game_pk'}
        )
        db_session.execute(stmt)

        # --- Upsert Game State ---
        linescore = live_data.get("linescore", {})
        current_play = live_data.get("plays", {}).get("currentPlay", {})
        about = current_play.get("about", {})
        matchup = current_play.get("matchup", {})
        runners = current_play.get("runners", [])

        runner_ids = {r.get("movement", {}).get("end"): r.get("details", {}).get("runner", {}).get("id") for r in runners if r.get("movement", {}).get("end")}

        game_state_info = {
            "game_pk": game_pk,
            "inning": linescore.get("currentInning"),
            "top_bottom": "Top" if linescore.get("isTopInning") else "Bottom",
            "outs": about.get("outs"),
            "balls": matchup.get("count", {}).get("balls"),
            "strikes": matchup.get("count", {}).get("strikes"),
            "runner_on_first_id": runner_ids.get("1B"),
            "runner_on_second_id": runner_ids.get("2B"),
            "runner_on_third_id": runner_ids.get("3B"),
            "current_batter_id": matchup.get("batter", {}).get("id"),
            "current_pitcher_id": matchup.get("pitcher", {}).get("id"),
            "home_score": linescore.get("teams", {}).get("home", {}).get("runs"),
            "away_score": linescore.get("teams", {}).get("away", {}).get("runs"),
            "last_updated": datetime.utcnow()
        }

        stmt_gs = pg_insert(GameState).values(game_state_info)
        stmt_gs = stmt_gs.on_conflict_do_update(
            index_elements=['game_pk'],
            set_={k: getattr(stmt_gs.excluded, k) for k in game_state_info if k != 'game_pk'}
        )
        db_session.execute(stmt_gs)

        # --- Upsert Players/Teams (Basic - could be expanded) ---
        # Extract players and teams from gameData and potentially liveData
        # Use similar pg_insert().on_conflict_do_nothing() or on_conflict_do_update()
        # This part can get complex quickly depending on how much info to store

        db_session.commit()
        logging.info(f"Processed message for game {game_pk}, offset {message.offset}")

    except json.JSONDecodeError as e:
        logging.error(f"Failed to decode JSON message: {e}")
    except SQLAlchemyError as e:
        logging.error(f"Database error processing message: {e}")
        db_session.rollback() # Rollback on error
    except Exception as e:
        logging.error(f"Unexpected error processing message: {e}")
        db_session.rollback()


# --- Main Consumer Loop ---
def main():
    """Main function to run the Kafka consumer loop."""
    logging.info("Starting Real-time Transformer...")
    _, db_session_maker = connect_db()
    consumer = create_kafka_consumer()

    if not consumer:
        logging.critical("Failed to create Kafka consumer. Exiting.")
        return

    logging.info("Consumer started. Waiting for messages...")
    while True: # Keep running indefinitely
        db_session = db_session_maker()
        try:
            for message in consumer:
                # logging.debug(f"Received message: {message.topic}:{message.partition}:{message.offset} key={message.key}")
                process_message(message, db_session)
                consumer.commit() # Commit offset after successful processing
        except KeyboardInterrupt:
            logging.info("Shutdown signal received. Closing consumer.")
            break
        except Exception as e:
            # Log unexpected errors in the consumer loop itself
            logging.error(f"Error in consumer loop: {e}")
            # Consider adding a delay before restarting the loop on certain errors
            time.sleep(5)
        finally:
            db_session.close()

    consumer.close()
    logging.info("Real-time Transformer stopped.")


if __name__ == "__main__":
    main()
