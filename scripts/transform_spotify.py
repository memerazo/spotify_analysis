import re
from pathlib import Path
import unidecode
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from scripts.grammys_connection import read_grammy_table


def read_spotify_csv(path: str) -> pd.DataFrame:
    """
    Reads the raw Spotify CSV and drops any unnamed columns.

    Parameters
    ----------
    path : str
        Path to the raw CSV file.

    Returns
    -------
    pd.DataFrame
        Cleaned DataFrame.
    """
    df = pd.read_csv(path)
    df.drop(columns=[col for col in df.columns if "Unnamed" in col], inplace=True)
    return df


def load_dataset(csv_path):
    """
    Load the dataset from a CSV file and drop unnamed columns.

    Parameters:
        csv_path (str): Path to the CSV file.

    Returns:
        pd.DataFrame: Cleaned DataFrame with unnamed columns removed.
    """
    df = pd.read_csv(csv_path)
    df.drop(columns=[col for col in df.columns if "Unnamed" in col], inplace=True)
    return df


def show_basic_info(df):
    """
    Print basic information about the dataset including shape, head, and data types.

    Parameters:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        None
    """
    print("Shape:", df.shape)
    print("\nFirst rows:")
    print(df.head())
    print("\nData types:")
    print(df.dtypes)


def check_nulls(df):
    """
    Display the number of null values for each column in the dataset.

    Parameters:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        pd.Series: Series with null value counts per column.
    """
    return df.isnull().sum()


def drop_nulls(df):
    """
    Remove all rows with null values from the dataset.

    Parameters:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with null rows removed.
    """
    return df.dropna()


def drop_duplicates(df):
    """
    Remove all duplicate rows from the dataset.

    Parameters:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: DataFrame without duplicate rows.
    """
    return df.drop_duplicates()


def filter_invalid_popularity(df, threshold=0):
    """
    Filters rows with popularity below a specified threshold.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing a 'popularity' column.
    threshold : int or float, optional
        Minimum allowed popularity value (default is 0).

    Returns
    -------
    pd.DataFrame
        Filtered DataFrame with valid popularity values.
    """
    return df[df["popularity"] > threshold].copy()


def filter_invalid_loudness(df, min_loudness=-60, max_loudness=0):
    """
    Filters out rows with 'loudness' values outside the specified range.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing the 'loudness' column.
    min_loudness : float, optional
        Minimum allowed loudness (default: -60).
    max_loudness : float, optional
        Maximum allowed loudness (default: 0).

    Returns
    -------
    pd.DataFrame
        Filtered DataFrame with valid loudness values.
    """
    return df[(df["loudness"] >= min_loudness) & (df["loudness"] <= max_loudness)].copy()


def convert_duration_to_minutes(df):
    """
    Converts duration from milliseconds to minutes and drops the original column.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing the 'duration_ms' column.

    Returns
    -------
    pd.DataFrame
        DataFrame with a new 'duration_min' column.
    """
    df = df.copy()
    df["duration_min"] = df["duration_ms"] / 60000
    df.drop(columns=["duration_ms"], inplace=True)
    return df


def encode_binary_columns(df, columns):
    """
    Encodes binary columns as integers (0 or 1).

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with binary columns.
    columns : list of str
        Names of the columns to encode.

    Returns
    -------
    pd.DataFrame
        DataFrame with encoded binary columns.
    """
    df = df.copy()
    for col in columns:
        df[col] = df[col].astype(int)
    return df


def map_key_column(df):
    """
    Maps numeric 'key' values to musical note names.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing the 'key' column.

    Returns
    -------
    pd.DataFrame
        DataFrame with a new 'key_name' column.
    """
    key_map = {
        0: "C", 1: "C#", 2: "D", 3: "D#", 4: "E", 5: "F",
        6: "F#", 7: "G", 8: "G#", 9: "A", 10: "A#", 11: "B"
    }
    df = df.copy()
    df["key_name"] = df["key"].map(key_map)
    return df


def categorize_genre(genre):
    """
    Categorizes a specific genre into a general music genre category.

    Parameters
    ----------
    genre : str
        Genre name.

    Returns
    -------
    str
        General genre category.
    """
    pop_genres = ["pop", "dance pop", "latin pop", "electropop", "k-pop", "post-teen pop"]
    hiphop_genres = ["hip hop", "rap", "trap", "gangsta rap"]
    edm_genres = ["edm", "electro house", "house", "big room", "progressive house"]
    rock_genres = ["rock", "alternative rock", "classic rock", "hard rock", "indie rock"]
    rnb_genres = ["r&b", "soul", "neo soul"]
    reggaeton_genres = ["reggaeton", "latin", "trap latino"]
    country_genres = ["country", "modern country rock"]
    jazz_genres = ["jazz", "vocal jazz", "smooth jazz"]

    g = genre.lower()
    if any(pg in g for pg in pop_genres):
        return "Pop"
    elif any(hh in g for hh in hiphop_genres):
        return "Hip-Hop"
    elif any(e in g for e in edm_genres):
        return "EDM"
    elif any(r in g for r in rock_genres):
        return "Rock"
    elif any(rb in g for rb in rnb_genres):
        return "R&B"
    elif any(rg in g for rg in reggaeton_genres):
        return "Reggaeton"
    elif any(c in g for c in country_genres):
        return "Country"
    elif any(j in g for j in jazz_genres):
        return "Jazz"
    else:
        return "Other"


def apply_genre_category(df):
    """
    Applies genre categorization to the 'track_genre' column.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with the 'track_genre' column.

    Returns
    -------
    pd.DataFrame
        DataFrame with a new 'genre_category' column.
    """
    df = df.copy()
    df["genre_category"] = df["track_genre"].apply(categorize_genre)
    return df


def categorize_energy(df):
    """
    Categorizes the 'energy' column into Low, Medium, and High levels.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with the 'energy' column.

    Returns
    -------
    pd.DataFrame
        DataFrame with a new 'energy_level' column.
    """
    df = df.copy()
    bins = [0, 0.3, 0.6, 1.0]
    labels = ["Low", "Medium", "High"]
    df["energy_level"] = pd.cut(df["energy"], bins=bins, labels=labels)
    return df


def clean_string(s):
    """
    Cleans a string for fuzzy matching.

    Parameters
    ----------
    s : str

    Returns
    -------
    str
        Cleaned version of the input string.
    """
    if pd.isnull(s):
        return ""
    s = s.lower()
    s = unidecode.unidecode(s)
    s = re.sub(r'\(.*?\)|\[.*?\]|\"|\'', '', s)
    s = re.sub(r'feat\.|ft\.|&', '', s)
    s = re.sub(r'[^a-z0-9\s]', '', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def normalize_track_names(df):
    """
    Applies string cleaning to track and artist names.

    Parameters
    ----------
    df : pd.DataFrame

    Returns
    -------
    pd.DataFrame
    """
    df = df.copy()
    df["track_name_clean"] = df["track_name"].apply(clean_string)
    df["artists_clean"] = df["artists"].apply(clean_string)
    return df


def preprocess_spotify_data_task(**kwargs):
    """
    Applies the full Spotify preprocessing pipeline.
    Expects a dictionary from XCom with raw data.
    """
    ti = kwargs['ti']
    raw_dict = ti.xcom_pull(task_ids='read_spotify_data_task')

    if raw_dict is None:
        raise Exception("No valid data received for processing.")

    df = pd.DataFrame(raw_dict)

    output_path = Path(__file__).resolve().parent.parent / "data/processed/spotify_processed.csv"

    df = filter_invalid_popularity(df)
    df = filter_invalid_loudness(df)
    df = convert_duration_to_minutes(df)
    df = encode_binary_columns(df, columns=["explicit", "mode"])
    df = map_key_column(df)
    df = apply_genre_category(df)
    df = categorize_energy(df)
    df = normalize_track_names(df)

    try:
        df.to_csv(output_path, index=False)
        print(f"Processed data saved to: {output_path}")
    except Exception as e:
        print(f"Error saving file: {e}")
        raise

    return df.to_dict()


def run(df, save_path="data/spotify_clean.csv"):
    """
    Run the full preprocessing pipeline on the Spotify dataset.

    Parameters
    ----------
    df : pd.DataFrame
        Raw Spotify dataset.
    save_path : str
        Path to save the cleaned dataset.
    """
    df = filter_invalid_popularity(df)
    df = filter_invalid_loudness(df)
    df = convert_duration_to_minutes(df)
    df = encode_binary_columns(df, columns=["explicit", "mode"])
    df = map_key_column(df)
    df = apply_genre_category(df)
    df = categorize_energy(df)
    df = normalize_track_names(df)
    df.to_csv(save_path, index=False)
