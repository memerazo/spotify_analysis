import logging
import pandas as pd
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from pathlib import Path

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def store_to_drive(**kwargs):
    """
    Stores the sentiment analysis CSV file on Google Drive.

    Parameters
    ----------
    kwargs : dict
        Context dictionary from Airflow containing runtime information.
        Expects 'ti' (TaskInstance) to retrieve XCom data.

    Raises
    ------
    ValueError
        If the file path is not received or if reading the CSV fails.
    Exception
        If authentication with Google Drive or file upload fails.
    """
    ti = kwargs["ti"]
    file_path = ti.xcom_pull(task_ids="load_data_task", key="eda_path")

    if not file_path:
        logger.error("No file path received from load_data_task")
        raise ValueError("No file path received from load_data_task")

    try:
        df = pd.read_csv(file_path)
        logger.info(f"Loaded DataFrame from {file_path}")
    except Exception as e:
        logger.error(f"Error loading file {file_path}: {str(e)}")
        raise ValueError(f"Error loading file: {e}")

    credentials_dir = Path(__file__).resolve().parent.parent / "credentials"
    settings_path = credentials_dir / "settings.yaml"
    credentials_file = credentials_dir / "saved_credentials.json"

    try:
        logger.info("Configuring Google Drive authentication")
        gauth = GoogleAuth(settings_file=str(settings_path))

        if credentials_file.exists():
            logger.info(f"Loading credentials from {credentials_file}")
            gauth.LoadCredentialsFile(str(credentials_file))

        if gauth.credentials is None or gauth.access_token_expired:
            logger.info("Starting authentication")
            gauth.LocalWebserverAuth()
            gauth.SaveCredentialsFile(str(credentials_file))
            logger.info(f"Credentials saved to {credentials_file}")
        else:
            logger.info("Refreshing credentials")
            gauth.Refresh()
            logger.info("Credentials refreshed")

        drive = GoogleDrive(gauth)
    except Exception as e:
        logger.error(f"Authentication failed: {str(e)}")
        raise

    try:
        file_name = "merged_spotify_genius_sentiment.csv"
        logger.info(f"Uploading file {file_name} to Google Drive")

        file = drive.CreateFile({"title": file_name})
        file.SetContentFile(file_path)
        file.Upload()

        logger.info(f"File {file_name} uploaded successfully")
    except Exception as e:
        logger.error(f"Upload failed: {str(e)}")
        raise

    ti.xcom_push(key="drive_file_id", value=file.get("id"))


def store_drive_task(**kwargs):
    """
    Wrapper function for Airflow task execution.

    Parameters
    ----------
    kwargs : dict
        Airflow context dictionary.
    """
    return store_to_drive(**kwargs)
