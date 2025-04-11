import pandas as pd
import re
import unidecode
import os


def delete_unnecessary_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes the columns 'workers', 'winner', and 'img' from the DataFrame if present.

    Parameters
    ----------
    df : pd.DataFrame

    Returns
    -------
    pd.DataFrame
    """
    columns_to_drop = ["workers", "winner", "img"]
    return df.drop(columns=columns_to_drop, errors="ignore")


def fill_missing_artists(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replaces missing values in the 'artist' column with 'Unknown'.

    Parameters
    ----------
    df : pd.DataFrame

    Returns
    -------
    pd.DataFrame
    """
    df = df.copy()
    df["artist"] = df["artist"].fillna("Unknown")
    return df  


def clean_string(s: str) -> str:
    """
    Cleans a string for fuzzy matching by converting to lowercase, removing accents,
    punctuation, special characters, and common terms, and trimming whitespace.

    Parameters
    ----------
    s : str

    Returns
    -------
    str
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


def normalize_artist_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalizes the 'artist' column by converting text to lowercase and trimming whitespace.

    Parameters
    ----------
    df : pd.DataFrame

    Returns
    -------
    pd.DataFrame
    """
    df = df.copy()
    df["artist"] = df["artist"].str.lower().str.strip()
    return df


def normalize_nominee_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies string normalization to the 'nominee' and 'artist' fields 
    and stores results in new columns.

    Parameters
    ----------
    df : pd.DataFrame

    Returns
    -------
    pd.DataFrame
    """
    df = df.copy()
    df["nominee_clean"] = df["nominee"].apply(clean_string)
    df["artist_clean"] = df["artist"].apply(clean_string)
    return df


def main():
    """
    Executes the data transformation pipeline for the Grammy dataset.
    Loads the raw data, applies cleaning functions, and writes the output to file.
    """
    input_csv_path = "data/raw/the_grammy_awards.csv"
    output_csv_path = "data/processed/grammy_transformed.csv"

    if not os.path.exists(input_csv_path):
        raise FileNotFoundError(f"Input file not found at: {input_csv_path}")

    df = pd.read_csv(input_csv_path)

    if df.empty:
        print("Warning: DataFrame is empty.")
        return

    try:
        df = (
            df.pipe(delete_unnecessary_columns)
              .pipe(fill_missing_artists)
              .pipe(normalize_artist_names)
              .pipe(normalize_nominee_fields)
        )

        output_dir = os.path.dirname(output_csv_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)

        df.to_csv(output_csv_path, index=False)
        print(f"Successfully saved cleaned data to: {output_csv_path}")

    except Exception as e:
        print(f"Error during transformation: {str(e)}")
        raise
