import json
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path



def load_credentials(path="/home/user/etl_project/credentials/credentials.json"):
    """
    Load PostgreSQL connection credentials from a JSON file.

    Parameters
    ----------
    path : str
        Path to the JSON file containing credentials.

    Returns
    -------
    dict
        Dictionary containing loaded credentials.
    """
    with open(path) as f:
        return json.load(f)


def connect_postgres(creds):

    """
    Create a SQLAlchemy connection to a PostgreSQL database.

    Parameters
    ----------
    creds : dict
        Dictionary with PostgreSQL connection parameters under 'postgresql'.

    Returns
    -------
    sqlalchemy.engine.base.Engine
        SQLAlchemy connection engine.
    """
    url = f"postgresql://{creds['username']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['database']}"
    return create_engine(url)

def read_grammy_table(table_name="grammy_nominations"):
    
    """
    Reads the specified table from the PostgreSQL database into a DataFrame.

    Parameters
    ----------
    table_name : str
        Name of the table to read from the database.

    Returns
    -------
    pd.DataFrame
        The table as a DataFrame.
    """
    credentials_path = Path(__file__).resolve().parent.parent / "credentials" / "credentials.json"
    with open(credentials_path) as f:
        creds = json.load(f)["postgresql"]

    engine = connect_postgres(creds)
    df = pd.read_sql(f"SELECT * FROM {table_name}", engine)
    return df

def read_grammy_task(**kwargs):

    """
    Reads the Grammy table and pushes the DataFrame to Airflow XCom.

    Parameters
    ----------
    kwargs : dict
        Airflow context including TaskInstance.

    Returns
    -------
    None
    """
    df = read_grammy_table()
    kwargs['ti'].xcom_push(key='grammy_df', value=df.to_json())