import os
import sys
from pathlib import Path
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

BASE_DIR = str(Path(__file__).resolve().parent.parent)
sys.path.append(str(BASE_DIR))

from scripts.read_csv import read_spotify_csv
from scripts.grammys_connection import read_grammy_task
from scripts.transform_spotify import preprocess_spotify_data_task
from scripts.genius_enrichment import run_sentiment_task
from scripts.genius_lyrics import enrich_dataframe_with_genius
from scripts.transform_grammys import main
from scripts.merge_spotify_grammy import mainsg
from scripts.load import load_to_db_task
from scripts.store_data import store_to_drive

with DAG(
    dag_id="etl",
    description="ETL pipeline for cleaning Spotify dataset and Grammy data",
    schedule_interval=None,
    start_date=datetime(2025, 4, 8),
    catchup=False,
) as dag:

    read_spotify_task = PythonOperator(
        task_id="read_spotify_data_task",
        python_callable=read_spotify_csv,
        op_kwargs={'path': os.path.join(BASE_DIR, "data", "spotify_raw.csv")},
        provide_context=True,
    )

    transform_spotify_task = PythonOperator(
        task_id="preprocess_spotify_data_task",
        python_callable=preprocess_spotify_data_task,
        provide_context=True,
    )

    read_grammy_task = PythonOperator(
        task_id="read_grammy_task",
        python_callable=read_grammy_task,
        provide_context=True,
    )

    transform_grammy_task = PythonOperator(
        task_id="transform_grammy_task",
        python_callable=main,
    )

    merge_task = PythonOperator(
        task_id="merge_spotify_grammy_task",
        python_callable=mainsg,
        provide_context=True,
    )

    enrich_with_genius_task = PythonOperator(
        task_id="extract_api_merge_task",
        python_callable=enrich_dataframe_with_genius,
        op_kwargs={
            "title_col": "nominee_clean",
            "artist_col": "artist_clean",
            "credentials_path": "credentials/credentials.json",
            "delay": 1.0
        },
        provide_context=True,
    )

    sentiment_task = PythonOperator(
        task_id="sentiment_task",
        python_callable=run_sentiment_task,
        provide_context=True,
    )

    load_data_task = PythonOperator(
        task_id="load_data_task",
        python_callable=load_to_db_task,
        provide_context=True,
    )

    store_drive_task = PythonOperator(
        task_id="store_drive_task",
        python_callable=store_to_drive,
        provide_context=True,
    )

    read_spotify_task >> transform_spotify_task
    read_grammy_task >> transform_grammy_task
    [transform_spotify_task, transform_grammy_task] >> merge_task
    merge_task >> enrich_with_genius_task
    enrich_with_genius_task >> sentiment_task
    sentiment_task >> load_data_task
    load_data_task >> store_drive_task