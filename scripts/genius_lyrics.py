import requests
import logging
import json
import os
import sys
import pandas as pd
from pathlib import Path
from time import sleep
import lyricsgenius

def load_genius_token(path="credentials/credentials.json"):
    """
    Loads the Genius client access token from a credentials JSON file.

    Parameters
    ----------
    path : str
        Path to the credentials JSON file.

    Returns
    -------
    str
        Genius client access token.
    """
    with open(Path(path)) as f:
        creds = json.load(f)
    return creds["genius"]["client_access_token"]

def init_genius_client(token):
    """
    Initializes the Genius lyricsgenius client.

    Parameters
    ----------
    token : str
        Genius client access token.

    Returns
    -------
    lyricsgenius.Genius
        Initialized Genius client.
    """
    genius = lyricsgenius.Genius(token)
    genius.skip_non_songs = True
    genius.remove_section_headers = True
    genius.verbose = False
    return genius

def search_song_metadata(title, artist, token):
    """
    Uses Genius API REST to find song metadata.

    Parameters
    ----------
    title : str
        Song title.
    artist : str
        Artist name.
    token : str
        Genius API token.

    Returns
    -------
    dict or None
        Song metadata if found, else None.
    """
    url = "https://api.genius.com/search"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"q": f"{title} {artist}"}
    res = requests.get(url, headers=headers, params=params)

    if res.status_code == 200:
        hits = res.json()["response"]["hits"]
        if hits:
            return hits[0]["result"]
    return None

def fetch_song_details(song_id, token):
    """
    Fetches detailed song data from Genius API by song ID.

    Parameters
    ----------
    song_id : int
        Genius song ID.
    token : str
        Genius API token.

    Returns
    -------
    dict or None
        Detailed song data if found, else None.
    """
    url = f"https://api.genius.com/songs/{song_id}"
    headers = {"Authorization": f"Bearer {token}"}
    res = requests.get(url, headers=headers)

    if res.status_code == 200:
        return res.json()["response"]["song"]
    print(f"Failed to fetch song details for song_id {song_id}. Status code: {res.status_code}")
    return None

def fetch_lyrics_with_library(title, artist, genius_client):
    """
    Uses lyricsgenius to fetch the lyrics of a song.

    Parameters
    ----------
    title : str
        Song title.
    artist : str
        Artist name.
    genius_client : lyricsgenius.Genius
        Initialized Genius client.

    Returns
    -------
    str or None
        Song lyrics if found, else None.
    """
    try:
        song = genius_client.search_song(title, artist)
        return song.lyrics if song else None
    except Exception as e:
        print(f"Error fetching lyrics for {title} by {artist}: {e}")
        return None

def enrich_dataframe_with_genius(
    title_col: str = "nominee_clean",
    artist_col: str = "artist_clean",
    credentials_path: str = "/path/to/credentials.json",
    delay: float = 1.0,
    **kwargs
):
    """
    Enriches a DataFrame with Genius API data using XCom in Airflow.
    Keeps only specified columns and adds Genius metadata.

    Parameters
    ----------
    title_col : str
        Name of the column containing song titles (default: 'nominee_clean').
    artist_col : str
        Name of the column containing artist names (default: 'artist_clean').
    credentials_path : str
        Path to the Genius API credentials file.
    delay : float
        Delay between API requests to avoid rate limiting.
    kwargs : dict
        Airflow context, including TaskInstance (ti) for XCom.

    Returns
    -------
    str
        Path to the enriched CSV file.
    """
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='merge_spotify_grammy_task')

    logging.info(f"Received DataFrame with {len(df) if df is not None else 'None'} rows")
    logging.info(f"DataFrame columns: {list(df.columns) if df is not None else 'None'}")
    if df is None or df.empty:
        logging.error("Received an empty or None DataFrame from merge_spotify_grammy_task")
        raise ValueError("Cannot enrich an empty DataFrame")

    required_columns = [title_col, artist_col, 'year', 'title', 'published_at', 'category']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logging.error(f"Missing required columns: {missing_columns}. Available columns: {list(df.columns)}")
        raise ValueError(f"Missing required columns: {missing_columns}")

    df = df[required_columns].copy()

    try:
        token = load_genius_token(credentials_path)
        genius_client = init_genius_client(token)
    except Exception as e:
        logging.error(f"Failed to initialize Genius client: {str(e)}")
        raise

    metadata = {
        "genius_song_id": [],
        "genius_title": [],
        "genius_full_title": [],
        "genius_url": [],
        "genius_artist": [],
        "genius_release_date": [],
        "genius_lyrics_state": [],
        "genius_song_art_url": [],
        "genius_header_image_url": [],
        "genius_lyrics": []
    }

    for _, row in df.iterrows():
        title = row[title_col]
        artist = row[artist_col]

        try:
            result = search_song_metadata(title, artist, token)
            if result:
                song_id = result["id"]
                song_data = fetch_song_details(song_id, token)
                if song_data:
                    metadata["genius_song_id"].append(song_id)
                    metadata["genius_title"].append(song_data.get("title"))
                    metadata["genius_full_title"].append(song_data.get("full_title"))
                    metadata["genius_url"].append(song_data.get("url"))
                    metadata["genius_artist"].append(song_data.get("primary_artist", {}).get("name"))
                    metadata["genius_release_date"].append(song_data.get("release_date_for_display"))
                    metadata["genius_lyrics_state"].append(song_data.get("lyrics_state"))
                    metadata["genius_song_art_url"].append(song_data.get("song_art_image_url"))
                    metadata["genius_header_image_url"].append(song_data.get("header_image_url"))
                    lyrics = fetch_lyrics_with_library(title, artist, genius_client)
                    metadata["genius_lyrics"].append(lyrics)
                else:
                    logging.warning(f"No song details found for {title} by {artist}")
                    for key in metadata:
                        metadata[key].append(None)
            else:
                logging.warning(f"No search results for {title} by {artist}")
                for key in metadata:
                    metadata[key].append(None)
        except Exception as e:
            logging.error(f"Error processing {title} by {artist}: {str(e)}")
            for key in metadata:
                metadata[key].append(None)

        sleep(delay)

    for key, values in metadata.items():
        df[key] = values

    output_dir = "data/processed"
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "merged_spotify_genius.csv")

    try:
        df.to_csv(output_path, index=False)
        logging.info(f"Saved enriched data to {output_path}")
        logging.info(f"Enriched DataFrame preview:\n{df.head().to_string()}")
    except Exception as e:
        logging.error(f"Failed to save enriched data to {output_path}: {str(e)}")
        raise

    ti.xcom_push(key="enriched_path", value=output_path)

    return output_path