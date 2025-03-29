import os
import logging
from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, Boolean, Date
from sqlalchemy.orm import sessionmaker, Session, declarative_base
from sqlalchemy.exc import SQLAlchemyError
from pydantic import BaseModel, Field # For data validation and response models
from typing import List, Optional
from datetime import datetime, date

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
class Game(Base):
    __tablename__ = 'games'
    game_pk = Column(Integer, primary_key=True)
    home_team_id = Column(Integer)
    away_team_id = Column(Integer)
    game_datetime = Column(DateTime)
    venue_name = Column(String)
    game_status = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class GameState(Base):
    __tablename__ = 'game_state'
    game_pk = Column(Integer, primary_key=True)
    inning = Column(Integer)
    top_bottom = Column(String)
    outs = Column(Integer)
    balls = Column(Integer)
    strikes = Column(Integer)
    runner_on_first_id = Column(Integer, nullable=True)
    runner_on_second_id = Column(Integer, nullable=True)
    runner_on_third_id = Column(Integer, nullable=True)
    current_batter_id = Column(Integer, nullable=True)
    current_pitcher_id = Column(Integer, nullable=True)
    home_score = Column(Integer)
    away_score = Column(Integer)
    last_updated = Column(DateTime)

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

# Add more endpoints later (e.g., for players, teams, specific stats)

# --- Run with Uvicorn (for local testing) ---
# This part is usually not included when running with Docker CMD
# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=8000)
