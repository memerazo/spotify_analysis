import logging
import json
import pandas as pd
import os
import sys
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from scripts.grammys_connection import connect_postgres

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

def create_songs_analysis_db(engine):
    """
    Creates the 'merged_spotify_genius_sentiment' table in the connected database with defined columns.

    Args:
        engine: SQLAlchemy engine connected to PostgreSQL.

    Returns:
        engine: The same engine passed, without attempting to change the database.
    """
    create_table_query = """
    CREATE TABLE IF NOT EXISTS merged_spotify_genius_sentiment (
        year INT,
        title VARCHAR(255),
        published_at TIMESTAMP,
        category VARCHAR(255),
        nominee_clean VARCHAR(255),
        artist_clean VARCHAR(255),
        genius_song_id BIGINT,
        genius_title VARCHAR(255),
        genius_full_title VARCHAR(500),
        genius_url VARCHAR(500),
        genius_artist VARCHAR(255),
        genius_release_date VARCHAR(100),
        genius_lyrics_state VARCHAR(100),
        genius_song_art_url VARCHAR(500),
        genius_header_image_url VARCHAR(500),
        genius_lyrics TEXT
    );
    """
    try:
        with engine.connect() as conn:
            conn.execute(text(create_table_query))
            logger.info("Table 'merged_spotify_genius_sentiment' created or already exists with updated schema.")
    except SQLAlchemyError as e:
        logger.error(f"Failed to create table: {str(e)}")
        raise
    return engine  

def load_to_db(ti):
    """
    Loads the DataFrame into PostgreSQL and saves a copy for EDA.

    Args:
        ti: Airflow TaskInstance for accessing XCom.

    Returns:
        str: Path to the saved EDA file.
    """
    sentiment_file_path = ti.xcom_pull(task_ids='sentiment_task', key='sentiment_output')
    if not sentiment_file_path:
        logger.error("No path received from sentiment_task with key 'sentiment_output'")
        raise ValueError("No path received from sentiment_task")
    
    try:
        df = pd.read_csv(sentiment_file_path)
        logger.info(f"File loaded from {sentiment_file_path} with {len(df)} rows")
    except Exception as e:
        logger.error(f"Error reading file {sentiment_file_path}: {str(e)}")
        raise ValueError(f"Error reading file: {e}")
    
    if df.empty:
        logger.error("Loaded DataFrame is empty")
        raise ValueError("DataFrame is empty")
    
    try:
        credentials_path = Path(__file__).resolve().parent.parent / "credentials" / "credentials.json"
        with open(credentials_path) as f:
            creds = json.load(f)["postgresql"]
    except Exception as e:
        logger.error(f"Error loading credentials from {credentials_path}: {str(e)}")
        raise ValueError(f"Error loading credentials: {e}")
    
    default_engine = connect_postgres(creds)
    engine = create_songs_analysis_db(default_engine)
    
    eda_path = Path(__file__).resolve().parent.parent / "data" / "eda" / "spotify_genius_sentiment_eda.csv"
    eda_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        df.to_sql("merged_spotify_genius_sentiment", engine, if_exists='replace', index=False)
        df.to_csv(eda_path, index=False)
        logger.info(f"Data loaded to database and EDA saved at {eda_path}")
    except Exception as e:
        logger.error(f"Error loading data or saving file: {str(e)}")
        raise RuntimeError(f"Error loading data or saving file: {e}")
    finally:
        engine.dispose()
        default_engine.dispose()
    
    ti.xcom_push(key="eda_path", value=str(eda_path))
    return str(eda_path)

def load_to_db_task(**kwargs):
    """
    Wrapper function for execution in Airflow.

    Args:
        kwargs: Airflow context including TaskInstance.

    Returns:
        str: Path to the saved EDA file.
    """
    ti = kwargs["ti"]
    return load_to_db(ti)
