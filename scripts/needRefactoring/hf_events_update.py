import os
import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from datetime import datetime
from colorama import Fore

# Constants
PROJECT_ID = "chitechdb"
DATASET_ID = "behavior"
TABLE_ID = "behavior.HF-Events"
SOURCE_FOLDER = "../dataUploaders/HF-Events"
DESTINATION_FOLDER = "../dataUploaders/archivedFiles"
COLUMN_MAPPINGS = {
    "date": "date",
    "student_id": "id",
    "student_name": "name",
    "grade": "grade",
    "race": "race",
    "ethnicity": "ethnicity",
    "gender": "gender",
    "event_group": "event_group",
    "event_type": "event_type",
    "class": "class",
    "teacher": "teacher",
    "is_resolved": "is_resolved",
}


def calculate_yog(grade):
    """Calculate year of graduation based on grade."""
    return {9: 2027, 10: 2026, 11: 2025, 12: 2024}.get(grade, None)


def clean_data(df):
    """Clean and process data."""
    df.drop(columns=df.columns[12:14], inplace=True)
    df.rename(columns=COLUMN_MAPPINGS, inplace=True)
    df["yog"] = df["grade"].apply(calculate_yog)
    return df


def delete_old_data_from_db(client):
    """Delete all data from the specified BigQuery table."""
    query = f"DELETE FROM `{PROJECT_ID}.{TABLE_ID}` WHERE TRUE"
    client.query(query).result()
    print(Fore.BLUE + "Data deletion completed.")


def upload_to_big_query(df):
    """Upload DataFrame to BigQuery."""
    schema = [
        {"name": key, "type": value}
        for key, value in {
            "date": "DATE",
            "id": "INTEGER",
            "name": "STRING",
            "grade": "INTEGER",
            "race": "STRING",
            "ethnicity": "STRING",
            "gender": "STRING",
            "event_group": "STRING",
            "event_type": "STRING",
            "class": "STRING",
            "teacher": "STRING",
            "is_resolved": "STRING",
            "yog": "INTEGER",
        }.items()
    ]
    print(Fore.BLUE + "Uploading data to BigQuery...")
    pandas_gbq.to_gbq(
        df,
        destination_table=TABLE_ID,
        project_id=PROJECT_ID,
        if_exists="append",
        table_schema=schema,
        progress_bar=True,
    )


def move_source_file_to_archive(csv_file, df):
    """Archive the processed CSV file."""
    current_datetime = datetime.now().strftime("%Y-%m-%d")
    new_file_name = f"HF-event-{current_datetime}.csv"
    df.to_csv(os.path.join(DESTINATION_FOLDER, new_file_name), index=False)
    os.remove(os.path.join(SOURCE_FOLDER, csv_file))
    print(Fore.GREEN + f"File '{csv_file}' has been archived as '{new_file_name}'.")


if __name__ == "__main__":
    print(Fore.RESET + "Starting script...")

    csv_files = [f for f in os.listdir(SOURCE_FOLDER) if f.endswith(".csv")]
    if csv_files:
        csv_file = csv_files[0]
        df = pd.read_csv(os.path.join(SOURCE_FOLDER, csv_file))
        print(Fore.RESET + f"Found file: {csv_file}")

        client = bigquery.Client(project=PROJECT_ID)
        if client:
            df = df.dropna(how="all")
            df = clean_data(df)
            delete_old_data_from_db(client)
            upload_to_big_query(df)
            move_source_file_to_archive(csv_file, df)
        else:
            print(Fore.RED + "Failed to initialize BigQuery client.")
    else:
        print(Fore.RED + "No CSV files found in the 'hf-events' folder.")
