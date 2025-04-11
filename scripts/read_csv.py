import os
import pandas as pd
from pathlib import Path

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
    try:
        BASE_DIR = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../..")
        )
        CSV_PATH = Path(__file__).resolve().parent.parent / "data" / "raw" / "spotify_dataset.csv"
        
        df = pd.read_csv(CSV_PATH)
        df.drop(columns=[col for col in df.columns if "Unnamed" in col], inplace=True)
        if df.empty:
            raise ValueError(f"No data found in {path}")
        print(f"Loaded Spotify data from {path}")
        return df
    except FileNotFoundError:
        print(f"File not found: {path}")
        raise
    except Exception as e:
        print(f"Error reading {path}: {e}")
        raise