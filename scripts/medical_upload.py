import os
from datetime import date, datetime

import pandas as pd
import pandas_gbq
from colorama import Fore
from google.cloud import bigquery

# Constants
PROJECT_ID = "chitechdb"
TABLE_ID = "student_info.medicalCompliance"
ROSTER_TABLE_ID = "student_info.roster"
SOURCE_FOLDER = "../dataUploaders/medicalCompliance"
DESTINATION_FOLDER = "../dataUploaders/archivedFiles"
ARCHIVE_NAME = "medicalCompliance"


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

    # Drop the "School" column
    df = df.drop(columns=["School"])

    # Extract student ID and reason from the "Notes" column
    df["id"] = df["Notes"].str.extract(r"Student # (\d+)").astype("Int64")
    # Extract reason which is at the end of the string
    df["reason"] = df["Notes"].str.extract(r"compliant for (.+)$")

    print(Fore.BLUE + "Getting data from BigQuery...")
    # Fetch names from chitechdb.student_info.roster using the extracted IDs
    client = bigquery.Client(project=PROJECT_ID)
    unique_ids = df["id"].unique()
    query = f"""
        SELECT id, name
        FROM `chitechdb.student_info.roster`
        WHERE id IN ({", ".join(map(str, unique_ids))})
    """
    names_df = client.query(query).to_dataframe()

    print(Fore.RESET + "Merging data...")

    # Merge the fetched names with the original DataFrame
    df = pd.merge(df, names_df, on="id", how="left")

    # Drop the original "Notes" column
    df = df.drop(columns=["Notes"])

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
        {"name": "id", "type": "STRING"},
        {"name": "name", "type": "STRING"},
        {"name": "reason", "type": "STRING"},
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
        delete_all_data_from_table()
        upload_to_big_query(df)
        archive_source_file(csv_file, df)
