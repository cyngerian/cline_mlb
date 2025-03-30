import json
import os
import logging
import time
from kafka import KafkaConsumer
from sqlalchemy import (
    create_engine, Column, Integer, String, DateTime, Float, Boolean, Date, MetaData,
    ForeignKey, Text, PrimaryKeyConstraint # Added ForeignKey, Text, PrimaryKeyConstraint
)
from sqlalchemy.orm import sessionmaker, declarative_base, relationship # Added relationship
from sqlalchemy.dialects.postgresql import insert as pg_insert # For UPSERT functionality
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime, timezone # Added timezone

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

# --- Database Models ---
class Venue(Base):
    __tablename__ = 'venues'
    venue_id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    city = Column(String)
    state = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    games = relationship("Game", back_populates="venue")

class Team(Base):
    __tablename__ = 'teams'
    team_id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    abbreviation = Column(String)
    league = Column(String)
    division = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    home_games = relationship("Game", foreign_keys="Game.home_team_id", back_populates="home_team")
    away_games = relationship("Game", foreign_keys="Game.away_team_id", back_populates="away_team")

class Player(Base):
    __tablename__ = 'players'
    player_id = Column(Integer, primary_key=True)
    full_name = Column(String, nullable=False)
    primary_position = Column(String)
    birth_date = Column(Date)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships for plays
    plays_as_batter = relationship("Play", foreign_keys="Play.batter_id", back_populates="batter")
    plays_as_pitcher = relationship("Play", foreign_keys="Play.pitcher_id", back_populates="pitcher")
    plays_on_1b = relationship("Play", foreign_keys="Play.runner_on_first_id", back_populates="runner_on_first")
    plays_on_2b = relationship("Play", foreign_keys="Play.runner_on_second_id", back_populates="runner_on_second")
    plays_on_3b = relationship("Play", foreign_keys="Play.runner_on_third_id", back_populates="runner_on_third")
    # Relationships for game state
    game_states_as_batter = relationship("GameState", foreign_keys="GameState.current_batter_id", back_populates="current_batter")
    game_states_as_pitcher = relationship("GameState", foreign_keys="GameState.current_pitcher_id", back_populates="current_pitcher")
    game_states_on_1b = relationship("GameState", foreign_keys="GameState.runner_on_first_id", back_populates="runner_on_first_gs")
    game_states_on_2b = relationship("GameState", foreign_keys="GameState.runner_on_second_id", back_populates="runner_on_second_gs")
    game_states_on_3b = relationship("GameState", foreign_keys="GameState.runner_on_third_id", back_populates="runner_on_third_gs")


class Game(Base):
    __tablename__ = 'games'
    game_pk = Column(Integer, primary_key=True)
    home_team_id = Column(Integer, ForeignKey('teams.team_id'))
    away_team_id = Column(Integer, ForeignKey('teams.team_id'))
    venue_id = Column(Integer, ForeignKey('venues.venue_id')) # Added FK
    game_datetime = Column(DateTime(timezone=True)) # Added timezone=True
    venue_name = Column(String) # Keep for convenience, though venue_id is FK
    game_status = Column(String) # e.g., Preview, Live, Final
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)) # Use timezone aware default
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc)) # Use timezone aware default/onupdate

    # Relationships
    home_team = relationship("Team", foreign_keys=[home_team_id], back_populates="home_games")
    away_team = relationship("Team", foreign_keys=[away_team_id], back_populates="away_games")
    venue = relationship("Venue", back_populates="games")
    game_state = relationship("GameState", back_populates="game", uselist=False) # One-to-one
    plays = relationship("Play", back_populates="game")


class GameState(Base):
    __tablename__ = 'game_state'
    game_pk = Column(Integer, ForeignKey('games.game_pk'), primary_key=True)
    inning = Column(Integer)
    top_bottom = Column(String(10)) # Limit string length
    outs = Column(Integer)
    balls = Column(Integer)
    strikes = Column(Integer)
    runner_on_first_id = Column(Integer, ForeignKey('players.player_id'), nullable=True)
    runner_on_second_id = Column(Integer, ForeignKey('players.player_id'), nullable=True)
    runner_on_third_id = Column(Integer, ForeignKey('players.player_id'), nullable=True)
    current_batter_id = Column(Integer, ForeignKey('players.player_id'), nullable=True)
    current_pitcher_id = Column(Integer, ForeignKey('players.player_id'), nullable=True)
    home_score = Column(Integer)
    away_score = Column(Integer)
    last_updated = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc)) # Use timezone aware default/onupdate

    # Relationships
    game = relationship("Game", back_populates="game_state")
    runner_on_first_gs = relationship("Player", foreign_keys=[runner_on_first_id], back_populates="game_states_on_1b")
    runner_on_second_gs = relationship("Player", foreign_keys=[runner_on_second_id], back_populates="game_states_on_2b")
    runner_on_third_gs = relationship("Player", foreign_keys=[runner_on_third_id], back_populates="game_states_on_3b")
    current_batter = relationship("Player", foreign_keys=[current_batter_id], back_populates="game_states_as_batter")
    current_pitcher = relationship("Player", foreign_keys=[current_pitcher_id], back_populates="game_states_as_pitcher")


# New table for individual plays
class Play(Base):
    __tablename__ = 'plays'
    # Composite primary key based on game and play indices from the feed
    game_pk = Column(Integer, ForeignKey('games.game_pk'), primary_key=True)
    at_bat_index = Column(Integer, primary_key=True)
    play_event_index = Column(Integer, primary_key=True) # Index of the last event in playEvents array

    inning = Column(Integer, nullable=False)
    top_bottom = Column(String(10), nullable=False)
    batter_id = Column(Integer, ForeignKey('players.player_id'), nullable=False)
    pitcher_id = Column(Integer, ForeignKey('players.player_id'), nullable=False)
    result_type = Column(String(50)) # e.g., 'atBat', 'action'
    result_event = Column(String(50)) # e.g., 'Strikeout', 'Home Run', 'Walk'
    result_description = Column(Text)
    rbi = Column(Integer)
    is_scoring_play = Column(Boolean)
    # Hit data (nullable as not all plays are hits)
    hit_data_launch_speed = Column(Float, nullable=True)
    hit_data_launch_angle = Column(Float, nullable=True)
    hit_data_total_distance = Column(Float, nullable=True)
    # Timestamps
    play_start_time = Column(DateTime(timezone=True), nullable=True)
    play_end_time = Column(DateTime(timezone=True), nullable=True)
    # Record creation/update time
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    # Relationships
    game = relationship("Game", back_populates="plays")
    batter = relationship("Player", foreign_keys=[batter_id], back_populates="plays_as_batter")
    pitcher = relationship("Player", foreign_keys=[pitcher_id], back_populates="plays_as_pitcher")

    # Define composite primary key constraint explicitly
    __table_args__ = (PrimaryKeyConstraint('game_pk', 'at_bat_index', 'play_event_index'),)

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
        # --- Upsert Venue (if needed) ---
        venue_data = game_data.get("venue", {})
        venue_id = venue_data.get("id")
        if venue_id:
            venue_info = {
                "venue_id": venue_id,
                "name": venue_data.get("name"),
                "city": venue_data.get("link"), # Link often contains city/state, parse later if needed
                "state": None # Requires parsing link or separate lookup
            }
            stmt_venue = pg_insert(Venue).values(venue_info)
            # Update name/city/state if they change (unlikely but possible)
            stmt_venue = stmt_venue.on_conflict_do_update(
                index_elements=['venue_id'],
                set_={
                    "name": stmt_venue.excluded.name,
                    "city": stmt_venue.excluded.city,
                    "state": stmt_venue.excluded.state,
                    "updated_at": datetime.now(timezone.utc)
                }
            )
            db_session.execute(stmt_venue)
        else:
            venue_id = None # Ensure venue_id is None if not found

        # --- Upsert Game Info ---
        game_info = {
            "game_pk": game_pk,
            "home_team_id": game_data.get("teams", {}).get("home", {}).get("id"),
            "away_team_id": game_data.get("teams", {}).get("away", {}).get("id"),
            "venue_id": venue_id, # Use the extracted/upserted venue_id
            "game_datetime": game_data.get("datetime", {}).get("dateTime"), # Assuming ISO format
            "venue_name": game_data.get("venue", {}).get("name"), # Keep for convenience
            "game_status": game_data.get("status", {}).get("abstractGameState"),
            "updated_at": datetime.now(timezone.utc)
        }
        # Convert datetime string if present
        if game_info["game_datetime"]:
            try:
                # Use fromisoformat and ensure timezone aware
                game_info["game_datetime"] = datetime.fromisoformat(game_info["game_datetime"].replace('Z', '+00:00'))
            except (ValueError, TypeError):
                logging.warning(f"Could not parse game_datetime: {game_info['game_datetime']}")
                game_info["game_datetime"] = None # Or handle differently

        stmt_game = pg_insert(Game).values(game_info)
        stmt_game = stmt_game.on_conflict_do_update(
            index_elements=['game_pk'],
            set_={k: getattr(stmt_game.excluded, k) for k in game_info if k != 'game_pk'}
        )
        db_session.execute(stmt_game)

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
            "last_updated": datetime.now(timezone.utc)
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
        # TODO: Add basic Team/Player upserts here if needed for FK constraints,
        # or ensure they are populated by a separate process/Airflow DAG.

        db_session.commit()
        logging.info(f"Processed message for game {game_pk}, offset {message.offset}")

    # except json.JSONDecodeError as e: # This is handled by the Kafka consumer deserializer
    #     logging.error(f"Failed to decode JSON message: {e}")
    except SQLAlchemyError as e:
        logging.error(f"Database error processing message offset {message.offset} for game {game_pk}: {e}")
        db_session.rollback() # Rollback on DB error
    except Exception as e:
        logging.error(f"Unexpected error processing message offset {message.offset} for game {game_pk}: {e}")
        db_session.rollback() # Rollback on other errors


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
