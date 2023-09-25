import os
from datetime import datetime

import pandas as pd
import pandas_gbq
from colorama import Fore
from google.cloud import bigquery

# Constants
PROJECT_ID = "chitechdb"
TABLE_ID = "attendance.at-report"
SOURCE_FOLDER = "../dataUploaders/at-report"
DESTINATION_FOLDER = "../dataUploaders/archivedFiles"
ARCHIVE_NAME = "at-report"
COLUMN_MAPPINGS = {
    "Student > Name": "name",
    "Student > Student ID": "id",
    "Date": "date",
    "Code": "code",
    "Master > Class": "course",
    "Master > Description": "class",
    "Period": "period",
    "Tardy?": "tardy",
    "Absent?": "absent",
    "Student > YOG": "yog",
}

def load_csv_from_source():
    """Load CSV from source folder."""
    csv_files = [f for f in os.listdir(SOURCE_FOLDER) if f.endswith(".csv")]
    if csv_files:
        csv_file = csv_files[0]
        return pd.read_csv(os.path.join(SOURCE_FOLDER, csv_file)).dropna(how="all"), csv_file
    else:
        print(Fore.RED + f"No CSV files found in the '{SOURCE_FOLDER}' folder.")
        return None, None

def process_and_clean_data(df):
    """
    Cleans, processes and renames columns.
    
    Args:
    - df (DataFrame): Original DataFrame

    Returns:
    - DataFrame: Processed DataFrame
    """
    print(Fore.RESET + "Processing data...")

    # Rename columns
    df.rename(columns=COLUMN_MAPPINGS, inplace=True)

    # Convert date and derive related columns
    df["date"] = df["date"].apply(convert_to_standard_date)
    df["sy"] = df["date"].apply(get_sy)
    df["semester"] = df["date"].apply(get_semester)
    
    return df

def delete_old_data_from_db(client, min_date, max_date):
    """Deletes data within a specified date range."""
    query = f"DELETE FROM `{TABLE_ID}` WHERE date BETWEEN '{min_date}' AND '{max_date}'"
    client.query(query).result()
    print(Fore.BLUE + "Data deletion completed.")

def convert_to_standard_date(date_str):
    month, day, year = map(int, date_str.split("/"))
    year += 2000 if year < 100 else 0
    return f"{year:04d}-{month:02d}-{day:02d}"

def get_sy(date):
    year, month = map(int, date.split("-")[:2])
    return f"SY{year}" if 8 <= month <= 12 else f"SY{year - 1}"

def get_semester(date):
    _, month, _ = map(int, date.split("-"))
    return "S1" if month >= 8 else "S2"

def upload_to_big_query(df):
    """Upload DataFrame to BigQuery."""
    # Schema for BigQuery table
    schema = [
        {"name": "name", "type": "STRING"},
        {"name": "id", "type": "INTEGER"},
        {"name": "date", "type": "DATE"},
        {"name": "code", "type": "STRING"},
        {"name": "course", "type": "STRING"},
        {"name": "class", "type": "STRING"},
        {"name": "period", "type": "STRING"},
        {"name": "tardy", "type": "BOOLEAN"},
        {"name": "absent", "type": "BOOLEAN"},
        {"name": "yog", "type": "INTEGER"},
        {"name": "sy", "type": "STRING"},
        {"name": "semester", "type": "STRING"},
    ]
    pandas_gbq.to_gbq(df, destination_table=TABLE_ID, project_id=PROJECT_ID, if_exists="append", progress_bar=True, table_schema=schema)
    print(Fore.BLUE + "Data uploaded to BigQuery table.")

def archive_source_file(csv_file, df):
    """Archive the processed CSV file."""
    current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    new_file_name = f"{ARCHIVE_NAME}-{current_datetime}.csv"
    df.to_csv(os.path.join(DESTINATION_FOLDER, new_file_name), index=False)
    os.remove(os.path.join(SOURCE_FOLDER, csv_file))
    print(Fore.GREEN + f"File '{csv_file}' has been archived as '{new_file_name}'.")

if __name__ == "__main__":
    print(Fore.RESET + "Starting script...")
    df, csv_file = load_csv_from_source()

    if df is not None and csv_file:
        client = bigquery.Client(project=PROJECT_ID)
        
        df = process_and_clean_data(df)
        min_date, max_date = df["date"].min(), df["date"].max()
        delete_old_data_from_db(client, min_date, max_date)
        upload_to_big_query(df)
        archive_source_file(csv_file, df)
    else:
        print(Fore.RED + "Failed to initialize BigQuery client or no CSV found.")
