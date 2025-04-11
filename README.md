# ETL Pipeline for Spotify and Grammy Data Analysis

This project implements an ETL (Extract, Transform, Load) pipeline to process Spotify and Grammy datasets, enrich them with Genius lyrics data, perform sentiment analysis, and store the results in a PostgreSQL database. The pipeline is orchestrated using Apache Airflow and includes data cleaning, fuzzy matching, API integration, and sentiment analysis of song lyrics.

---

## 🧩 Project Overview

The pipeline performs the following steps:

- **Extract**:
  - Reads raw Spotify data from a CSV file.
  - Extracts Grammy nomination data from a PostgreSQL database.

- **Transform**:
  - Cleans and preprocesses both Spotify and Grammy datasets.
  - Merges the datasets using fuzzy matching to align Spotify tracks with Grammy nominations.
  - Enriches the merged dataset with song metadata and lyrics from the Genius API.
  - Performs sentiment analysis on the lyrics using TextBlob.

- **Load**:
  - Stores the final enriched dataset in a PostgreSQL database.
  - Saves a local copy for exploratory data analysis (EDA).

- **Store** *(optional)*:
  - Uploads the results to Google Drive if `store_data.py` is implemented.

---

## 🗂 Project Structure

```
etl_project/
├── airflow/                  # Airflow-related files (logs, database, etc.)
├── credentials/              # Sensitive credentials (e.g., credentials.json)
├── dags/                     # Airflow DAG definitions
│   └── dag.py                # Main DAG file for the ETL pipeline
├── data/                     # Data storage (raw, processed, and EDA files)
│   ├── eda/                  # Files for exploratory data analysis
│   ├── processed/            # Processed and merged datasets
│   └── raw/                  # Raw input data (e.g., spotify_dataset.csv)
├── notebooks/                # Jupyter notebooks for experimentation
├── scripts/                  # Python scripts for the ETL pipeline
│   ├── __pycache__/          
│   ├── genius_enrichment.py
│   ├── genius_lyrics.py
│   ├── grammys_connection.py
│   ├── load.py
│   ├── merge_spotify_grammy.py
│   ├── read_csv.py
│   ├── store_data.py
│   ├── transform_grammys.py
│   └── transform_spotify.py
├── venv/                     # Virtual environment
├── .gitignore                # Git ignore file
└── requirements.txt          # Project dependencies
```

---

## ✅ Prerequisites

- Python 3.8+
- Apache Airflow
- PostgreSQL
- Genius API Token
- Google Drive API credentials *(optional)*

---

## ⚙️ Setup Instructions

### 1. Clone the Repository

```bash
git clone <repository-url>
cd etl_project
```

### 2. Set Up a Virtual Environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Airflow

```bash
export AIRFLOW_HOME=$(pwd)/airflow
airflow db init
airflow webserver --port 8080
airflow scheduler
```

### 5. Set Up Credentials

Create a file at `credentials/credentials.json` with the following structure:

```json
{
  "postgresql": {
    "username": "your_username",
    "password": "your_password",
    "host": "your_host",
    "port": "your_port",
    "database": "your_database"
  },
  "genius": {
    "client_access_token": "your_genius_token"
  }
}
```

Ensure this file is included in `.gitignore`.

---

## 📥 Prepare the Data

- Place the raw Spotify dataset (`spotify_dataset.csv`) in `data/raw/`.
- Ensure the Grammy nominations data is stored in your PostgreSQL database under the table `grammy_nominations`.

---

## 🚀 Usage

### Access Airflow UI

Open your browser and go to [http://localhost:8080](http://localhost:8080).  
Default credentials:  
- **Username**: `airflow`  
- **Password**: `airflow`

### Trigger the DAG

- Locate the `etl` DAG.
- Turn it **On** and click **"Trigger DAG"**.

### Monitor the Pipeline

- Use the Airflow UI to track task status.
- Logs are available under `airflow/logs/`.

### Output

- Final dataset saved in PostgreSQL under `merged_spotify_genius_sentiment`.
- Copy for EDA saved at `data/eda/spotify_genius_sentiment_eda.csv`.
- If implemented, data is uploaded to Google Drive.

---

## 🔧 Pipeline Tasks

The `etl` DAG includes:

- `read_spotify_data_task`
- `preprocess_spotify_data_task`
- `read_grammy_task`
- `transform_grammy_task`
- `merge_spotify_grammy_task`
- `extract_api_merge_task`
- `sentiment_task`
- `load_data_task`
- `store_drive_task` *(optional)*

---

## 📦 Dependencies

Key packages in `requirements.txt` include:

- `pandas`
- `sqlalchemy`
- `lyricsgenius`
- `rapidfuzz`
- `textblob`
- `deep-translator`
- `apache-airflow`

---

## 🤝 Contributing

```bash
git checkout -b feature/your-feature-name
# Make your changes
git commit -m "Add your commit message"
git push origin feature/your-feature-name
```

Then create a Pull Request.

---

## 🙌 Acknowledgments

[[github.com/juandavdaza](https://github.com/JuanDavidDazaR)]


---

## 📝 Notes

- Customize data source citations as needed.
- Add testing, troubleshooting, or extension instructions in separate sections if required.
- Remove Google Drive instructions if `store_data.py` is not used.
