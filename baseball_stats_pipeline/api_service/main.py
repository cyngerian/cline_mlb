import os
import logging
from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, Boolean, Date, Text, ForeignKey
from sqlalchemy.orm import sessionmaker, Session, declarative_base, relationship
from sqlalchemy.exc import SQLAlchemyError
from pydantic import BaseModel, Field # For data validation and response models
from typing import List, Optional
from datetime import datetime, date, timezone # Added timezone

# --- Configuration ---
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
    try:
        # In a real app, consider connection pooling options
        engine = create_engine(DATABASE_URL, pool_pre_ping=True)
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        logging.info("Database connection pool established.")
        # Note: We assume tables are created by the transformer service or migrations
        # Base.metadata.create_all(bind=engine) # Usually not done in the API service
    except SQLAlchemyError as e:
        logging.critical(f"Database connection failed: {e}. API cannot start.")
        # In a real app, might retry or have better handling
        exit(1)

# Dependency to get DB session
def get_db():
    if SessionLocal is None:
        raise RuntimeError("Database not connected")
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# --- Database Models (Mirroring rt_transformer for querying) ---
# These should ideally be in a shared library/package
# Mirroring models from rt_transformer for querying
class Player(Base):
    __tablename__ = 'players'
    player_id = Column(Integer, primary_key=True)
    full_name = Column(String, nullable=False)
    # Add other fields as needed for API responses

class Game(Base):
    __tablename__ = 'games'
    game_pk = Column(Integer, primary_key=True)
    home_team_id = Column(Integer) # Add FK/relationship if needed
    away_team_id = Column(Integer) # Add FK/relationship if needed
    venue_id = Column(Integer) # Add FK/relationship if needed
    game_datetime = Column(DateTime(timezone=True))
    venue_name = Column(String)
    game_status = Column(String)
    created_at = Column(DateTime(timezone=True))
    updated_at = Column(DateTime(timezone=True))

    # Add relationship if querying related data via Game model
    # game_state = relationship("GameState", back_populates="game", uselist=False)
    # plays = relationship("Play", back_populates="game")


class GameState(Base):
    __tablename__ = 'game_state'
    game_pk = Column(Integer, primary_key=True) # FK defined in transformer, not strictly needed here unless joining
    inning = Column(Integer)
    top_bottom = Column(String)
    outs = Column(Integer)
    balls = Column(Integer)
    strikes = Column(Integer)
    runner_on_first_id = Column(Integer, nullable=True)
    runner_on_second_id = Column(Integer, nullable=True)
    runner_on_third_id = Column(Integer, nullable=True)
    current_batter_id = Column(Integer, nullable=True)
    current_pitcher_id = Column(Integer, ForeignKey('players.player_id'), nullable=True)
    home_score = Column(Integer)
    away_score = Column(Integer)
    last_updated = Column(DateTime(timezone=True))

    # Add relationships if needed
    # game = relationship("Game", back_populates="game_state")
    # current_batter = relationship("Player", foreign_keys=[current_batter_id])
    # current_pitcher = relationship("Player", foreign_keys=[current_pitcher_id])

# Mirror Play model
class Play(Base):
    __tablename__ = 'plays'
    game_pk = Column(Integer, ForeignKey('games.game_pk'), primary_key=True)
    at_bat_index = Column(Integer, primary_key=True)
    play_event_index = Column(Integer, primary_key=True)
    inning = Column(Integer, nullable=False)
    top_bottom = Column(String(10), nullable=False)
    batter_id = Column(Integer, ForeignKey('players.player_id'), nullable=False)
    pitcher_id = Column(Integer, ForeignKey('players.player_id'), nullable=False)
    result_type = Column(String(50))
    result_event = Column(String(50))
    result_description = Column(Text)
    rbi = Column(Integer)
    is_scoring_play = Column(Boolean)
    hit_data_launch_speed = Column(Float, nullable=True)
    hit_data_launch_angle = Column(Float, nullable=True)
    hit_data_total_distance = Column(Float, nullable=True)
    play_start_time = Column(DateTime(timezone=True), nullable=True)
    play_end_time = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True))

    # Relationships to potentially include batter/pitcher names
    batter = relationship("Player", foreign_keys=[batter_id])
    pitcher = relationship("Player", foreign_keys=[pitcher_id])
    game = relationship("Game") # Add back_populates if needed


# --- Pydantic Models (for API request/response validation) ---
class GameBase(BaseModel):
    game_pk: int
    home_team_id: Optional[int] = None
    away_team_id: Optional[int] = None
    game_datetime: Optional[datetime] = None
    venue_name: Optional[str] = None
    game_status: Optional[str] = None

class GameResponse(GameBase):
    created_at: datetime
    updated_at: datetime

    class Config:
        orm_mode = True # Enable reading data directly from ORM models

class GameStateBase(BaseModel):
    game_pk: int
    inning: Optional[int] = None
    top_bottom: Optional[str] = None
    outs: Optional[int] = None
    balls: Optional[int] = None
    strikes: Optional[int] = None
    runner_on_first_id: Optional[int] = None
    runner_on_second_id: Optional[int] = None
    runner_on_third_id: Optional[int] = None
    current_batter_id: Optional[int] = None
    current_pitcher_id: Optional[int] = None
    home_score: Optional[int] = None
    away_score: Optional[int] = None

class GameStateResponse(GameStateBase):
    last_updated: datetime

    class Config:
        orm_mode = True

# Pydantic model for Player info (subset)
class PlayerInfo(BaseModel):
    player_id: int
    full_name: str

    class Config:
        orm_mode = True

# Pydantic model for Home Run details
class HomeRunDetail(BaseModel):
    game_pk: int
    inning: int
    top_bottom: str
    batter: Optional[PlayerInfo] = None # Include nested batter info
    pitcher: Optional[PlayerInfo] = None # Include nested pitcher info
    rbi: Optional[int] = None
    hit_data_launch_speed: Optional[float] = None
    hit_data_launch_angle: Optional[float] = None
    hit_data_total_distance: Optional[float] = None
    play_description: Optional[str] = Field(None, alias="result_description") # Alias field
    play_start_time: Optional[datetime] = None
    play_end_time: Optional[datetime] = None

    class Config:
        orm_mode = True
        allow_population_by_field_name = True # Allow using alias

# --- FastAPI App Setup ---
app = FastAPI(title="Baseball Stats API", version="0.1.0")

@app.on_event("startup")
async def startup_event():
    connect_db()

# --- API Endpoints ---
@app.get("/", summary="Root endpoint", description="Simple health check endpoint.")
async def read_root():
    return {"message": "Welcome to the Baseball Stats API!"}

@app.get("/games", response_model=List[GameResponse], summary="Get Games", description="Retrieve a list of games, optionally filtered by status.")
async def get_games(status: Optional[str] = None, db: Session = Depends(get_db)):
    try:
        query = db.query(Game)
        if status:
            query = query.filter(Game.game_status == status)
        games = query.order_by(Game.game_datetime.desc()).limit(100).all() # Add limit
        return games
    except SQLAlchemyError as e:
        logging.error(f"Error fetching games: {e}")
        raise HTTPException(status_code=500, detail="Database error fetching games")
    except Exception as e:
        logging.error(f"Unexpected error fetching games: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/games/{game_pk}/state", response_model=GameStateResponse, summary="Get Live Game State", description="Retrieve the current state for a specific game.")
async def get_game_state(game_pk: int, db: Session = Depends(get_db)):
    try:
        game_state = db.query(GameState).filter(GameState.game_pk == game_pk).first()
        if game_state is None:
            raise HTTPException(status_code=404, detail="Game state not found")
        return game_state
    except SQLAlchemyError as e:
        logging.error(f"Error fetching game state for {game_pk}: {e}")
        raise HTTPException(status_code=500, detail="Database error fetching game state")
    except Exception as e:
        logging.error(f"Unexpected error fetching game state for {game_pk}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/homeruns", response_model=List[HomeRunDetail], summary="Get Home Runs", description="Retrieve a list of home runs recorded in the plays table.")
async def get_homeruns(
    limit: int = Query(100, ge=1, le=1000, description="Number of results to return"),
    db: Session = Depends(get_db)
):
    """
    Fetches home run plays from the database.
    Currently fetches all recorded home runs, ordered by time descending.
    """
    try:
        # Query the Play table, filtering for home runs
        # Join with Player table twice using aliases to get both batter and pitcher names
        BatterPlayer = relationship(Player) # Alias for batter join
        PitcherPlayer = relationship(Player) # Alias for pitcher join

        homerun_plays = (
            db.query(Play)
            .join(BatterPlayer, Play.batter_id == BatterPlayer.player_id)
            .join(PitcherPlayer, Play.pitcher_id == PitcherPlayer.player_id)
            .options(
                relationship(Play.batter.of_type(BatterPlayer)).load_only(Player.player_id, Player.full_name), # Eager load batter
                relationship(Play.pitcher.of_type(PitcherPlayer)).load_only(Player.player_id, Player.full_name) # Eager load pitcher
            )
            .filter(Play.result_event == 'Home Run')
            .order_by(Play.play_end_time.desc().nullslast(), Play.game_pk.desc(), Play.at_bat_index.desc(), Play.play_event_index.desc())
            .limit(limit)
            .all()
        )
        # The response_model will automatically handle the nested PlayerInfo
        return homerun_plays
    except SQLAlchemyError as e:
        logging.error(f"Error fetching home runs: {e}")
        raise HTTPException(status_code=500, detail="Database error fetching home runs")
    except Exception as e:
        logging.error(f"Unexpected error fetching home runs: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


# Add more endpoints later (e.g., for players, teams, specific stats)

# --- Run with Uvicorn (for local testing) ---
# This part is usually not included when running with Docker CMD
# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=8000)
