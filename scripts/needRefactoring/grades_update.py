import os
from datetime import date, datetime

import pandas as pd
import pandas_gbq
from colorama import Fore
from google.cloud import bigquery

# Constants
PROJECT_ID = "chitechdb"
TABLE_ID = "academics.grades"
ROSTER_TABLE_ID = "student_info.roster"
SOURCE_FOLDER = "../dataUploaders/grades"
DESTINATION_FOLDER = "../dataUploaders/archivedFiles"
ARCHIVE_NAME = "grades"
COLUMN_NAMES = [
    "yog",
    "name",
    "drop1",
    "drop2",
    "id",
    "course",
    "drop3",
    "teacher",
    "drop4",
    "averageAndLetter",
]


def load_csv_from_source():
    """Load CSV from source folder."""
    csv_files = [f for f in os.listdir(SOURCE_FOLDER) if f.endswith(".csv")]
    if csv_files:
        csv_file = csv_files[0]
        return (
            pd.read_csv(os.path.join(SOURCE_FOLDER, csv_file)).dropna(how="all"),
            csv_file,
        )
    else:
        print(Fore.RED + f"No CSV files found in the '{SOURCE_FOLDER}' folder.")
        return None, None


def clean_data(df):
    """
    Cleans and processes data.

    Args:
    - df (DataFrame): Original DataFrame

    Returns:
    - DataFrame: Processed DataFrame
    """
    print(Fore.RESET + "Cleaning data...")
    df = df.copy()
    df.columns = COLUMN_NAMES
    checkpoint = input("Enter the Checkpoint in 'Q# CP#' format: ")

    # Processing
    df["id"] = pd.to_numeric(df["id"], errors="coerce")
    df.dropna(subset=["id"], inplace=True)
    df[["gradePercent", "letterGrade"]] = df["averageAndLetter"].str.split(expand=True)
    df.drop(columns=["averageAndLetter"], inplace=True)
    today = date.today()
    df["checkpoint"] = f"SY24: {checkpoint}"
    df["date"] = today.strftime("%Y-%m-%d")
    df["sy"] = "SY24"
    df["id"] = df["id"].astype(int)
    df["yog"] = df["yog"].astype(int)
    df["gradePercent"] = df["gradePercent"].astype(float)
    df = df[
        [
            "checkpoint",
            "date",
            "id",
            "name",
            "yog",
            "course",
            "teacher",
            "gradePercent",
            "letterGrade",
            "sy",
        ]
    ]

    print(Fore.RESET + "Merging data...")
    return df


def delete_all_data_from_table():
    """Deletes all data from the specified BigQuery table."""
    print(Fore.BLUE + "Deleting old data from database to override...")
    client = bigquery.Client(project=PROJECT_ID)
    query = f"DELETE FROM `{TABLE_ID}` WHERE TRUE"
    client.query(query).result()
    print(Fore.BLUE + f"All data from {TABLE_ID} has been deleted.")


def archive_source_file(csv_file, df):
    """Archive the processed CSV file."""
    current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    new_file_name = f"{ARCHIVE_NAME}-{current_datetime}.csv"
    df.to_csv(os.path.join(DESTINATION_FOLDER, new_file_name), index=False)
    os.remove(os.path.join(SOURCE_FOLDER, csv_file))
    print(Fore.GREEN + f"File '{csv_file}' has been archived as '{new_file_name}'.")


def upload_to_big_query(df):
    """Upload DataFrame to BigQuery."""
    # Schema for BigQuery table
    schema = [
        {"name": "checkpoint", "type": "STRING"},
        {"name": "date", "type": "DATE"},
        {"name": "id", "type": "INTEGER"},
        {"name": "name", "type": "STRING"},
        {"name": "yog", "type": "INTEGER"},
        {"name": "course", "type": "STRING"},
        {"name": "teacher", "type": "STRING"},
        {"name": "gradePercent", "type": "FLOAT"},
        {"name": "letterGrade", "type": "STRING"},
        {"name": "sy", "type": "STRING"},
    ]

    print(Fore.BLUE + "Uploading data to BigQuery...")
    pandas_gbq.to_gbq(
        df,
        destination_table=TABLE_ID,
        project_id=PROJECT_ID,
        if_exists="append",
        progress_bar=True,
        table_schema=schema,
    )


if __name__ == "__main__":
    print(Fore.RESET + "Starting script...")
    df, csv_file = load_csv_from_source()

    if df is not None and csv_file:
        df = clean_data(df)
        # delete_all_data_from_table()  # Uncomment if you want to delete old data before uploading
        upload_to_big_query(df)
        archive_source_file(csv_file, df)
