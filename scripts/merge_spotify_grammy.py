from pathlib import Path
import pandas as pd
from rapidfuzz import fuzz, process

def load_datasets(spotify_path: str, grammy_path: str):
    """
    Loads cleaned Spotify and Grammy datasets from CSV files.

    Parameters
    ----------
    spotify_path : str
        Path to cleaned Spotify dataset.
    grammy_path : str
        Path to cleaned Grammy dataset.

    Returns
    -------
    tuple
        (spotify_df, grammy_df)
    """
    spotify_df = pd.read_csv(spotify_path)
    grammy_df = pd.read_csv(grammy_path)
    return spotify_df, grammy_df

def fuzzy_match_datasets(spotify_df: pd.DataFrame, grammy_df: pd.DataFrame,
                         output_path: str,
                         score_threshold=90, artist_threshold=80) -> pd.DataFrame:
    """
    Performs fuzzy matching between Spotify and Grammy datasets and saves results.

    Parameters
    ----------
    spotify_df : pd.DataFrame
        Spotify dataset.
    grammy_df : pd.DataFrame
        Grammy dataset.
    output_path : str
        Path to save merged dataset.
    score_threshold : int, optional
        Minimum score for track name matching (default is 90).
    artist_threshold : int, optional
        Minimum score for artist name matching (default is 80).

    Returns
    -------
    pd.DataFrame
        Merged dataset with matches.
    """
    matches = []
    grammy_songs = grammy_df.copy()
    spotify_titles = list(spotify_df["track_name_clean"])

    for _, row in grammy_songs.iterrows():
        nominee = row["nominee_clean"]
        artist = row["artist_clean"]

        result = process.extractOne(nominee, spotify_titles, scorer=fuzz.token_sort_ratio)

        if result:
            best_match, score, idx = result
            spotify_artist = spotify_df.loc[idx, "artists_clean"]
            artist_score = fuzz.token_sort_ratio(artist, spotify_artist)

            if score >= score_threshold and artist_score >= artist_threshold:
                match = row.to_dict()
                match.update({
                    "track_name": spotify_df.loc[idx, "track_name"],  
                    "artist": spotify_df.loc[idx, "artists"],
                    "track_id": spotify_df.loc[idx, "track_id"],
                    "match_score": score,
                    "artist_match_score": artist_score
                })
                matches.append(match)

    merged_df = pd.DataFrame(matches)
    merged_df.to_csv(output_path, index=False)
    print(f"{len(merged_df)} matches saved to: {output_path}")

    return merged_df

def mainsg():
    """
    Executes the merging process between Spotify and Grammy datasets using absolute paths.

    Returns
    -------
    pd.DataFrame
        Merged dataset.
    """
    root = Path(__file__).resolve().parent.parent
    spotify_path = root / "data" / "processed" / "spotify_cleaned.csv"
    grammy_path = root / "data" / "processed" / "grammy_cleaned.csv"
    output_path = root / "data" / "processed" / "merged_spotify_grammy.csv"

    print("Loading datasets...")
    spotify_df, grammy_df = load_datasets(str(spotify_path), str(grammy_path))

    print("Performing fuzzy matching...")
    merged_df = fuzzy_match_datasets(spotify_df, grammy_df, output_path=str(output_path))

    print(f"Done. {len(merged_df)} matches saved to {output_path}")
    
    return merged_df