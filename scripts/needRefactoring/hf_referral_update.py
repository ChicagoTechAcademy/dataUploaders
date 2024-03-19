import os
import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from datetime import datetime
from colorama import Fore

# Constants
PROJECT_ID = "chitechdb"
DATASET_ID = "behavior"
TABLE_ID = "behavior.HF-Referrals"
SOURCE_FOLDER = "../dataUploaders/HF-ref"
DESTINATION_FOLDER = "../dataUploaders/archivedFiles"
COLUMN_MAPPINGS = {
    "date": "date",
    "student_id": "id",
    "student_name": "name",
    "grade": "grade",
    "race": "race",
    "ethnicity": "ethnicity",
    "gender": "gender",
    "name": "type",
    "teacher_that_created": "teacher",
    "teacher_that_resolved": "resolved_by",
    "referral_comment": "referral_comment",
    "is_resolved": "is_resolved",
    "is_resolved_comment": "is_resolved_comment",
    "referral_outcome_type_name": "referral_outcome_type_name",
}


def calculate_yog(grade):
    """Calculate year of graduation based on grade."""
    return {9: 2027, 10: 2026, 11: 2025, 12: 2024}.get(grade, None)


def clean_data(df):
    """Clean and preprocess the dataframe."""
    df = df.drop(df.columns[13], axis=1)
    df = df.drop(df.columns[12], axis=1)
    df = df.drop(df.columns[10], axis=1)

    # Rename columns
    for old_col, new_col in COLUMN_MAPPINGS.items():
        if old_col in df.columns and new_col not in df.columns:
            df.rename(columns={old_col: new_col}, inplace=True)

    df["yog"] = df["grade"].apply(calculate_yog)

    return df


def delete_old_data_from_db(client):
    """Delete old data from BigQuery."""
    query = f"DELETE FROM `{PROJECT_ID}.{TABLE_ID}` WHERE TRUE"
    client.query(query).result()
    print("Data deletion completed.")


def upload_to_big_query(df):
    """Upload the processed DataFrame to BigQuery."""
    pandas_gbq.to_gbq(
        df,
        destination_table=TABLE_ID,
        project_id=PROJECT_ID,
        if_exists="append",
        table_schema=[
            {"name": "date", "type": "DATE"},
            {"name": "id", "type": "INTEGER"},
            {"name": "name", "type": "STRING"},
            {"name": "grade", "type": "INTEGER"},
            {"name": "race", "type": "STRING"},
            {"name": "ethnicity", "type": "STRING"},
            {"name": "gender", "type": "STRING"},
            {"name": "type", "type": "STRING"},
            {"name": "teacher", "type": "STRING"},
            {"name": "is_resolved", "type": "STRING"},
            {"name": "resolved_by", "type": "STRING"},
            {"name": "yog", "type": "INTEGER"},
        ],
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
    csv_files = [f for f in os.listdir(SOURCE_FOLDER) if f.endswith(".csv")]

    if csv_files:
        csv_file = csv_files[0]
        df = pd.read_csv(os.path.join(SOURCE_FOLDER, csv_file))
        client = bigquery.Client(project=PROJECT_ID)
        print(Fore.RESET + f"Found file: {csv_file}")

        if client:
            df = df.dropna(how="all")
            df = clean_data(df)
            delete_old_data_from_db(client)
            upload_to_big_query(df)
            move_source_file_to_archive(csv_file, df)
        else:
            print("Failed to initialize BigQuery client.")
    else:
        print("No CSV files found in the source folder.")
