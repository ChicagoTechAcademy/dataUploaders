import os
import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from datetime import datetime

# Constants
project_id = "chitechdb"
table_id = "academics.schedules"
source_folder = "../dataUploaders/schedules"
destination_folder = "../dataUploaders/archivedFiles"
archive_name = "schedules"

column_mappings = {
    "LastName": "lastName",
    "FirstName": "firstName",
    "Student ID": "id",
    "Grade": "grade",
    "Class": "classCode",
    "Description": "class",
    "Clssrm": "room",
    "Name": "teacher",
    "Schedule": "period",
}


def clean_data(df):
    """
    Processes the dataframe for cleanup tasks.
    """

    print("Cleaning data...")

    # Drop unnecessary columns
    df.drop(
        columns=[
            "MiddleName",
            "SpecialEdStus",
            "Homeroom",
            "Inclusion?",
            "SecType",
            "Total",
            "Max",
        ],
        inplace=True,
        errors="ignore",
    )
    print("Dropped unnecessary columns.")

    # Rename columns based on predefined mappings
    df.rename(columns=column_mappings, inplace=True, errors="raise")
    print(f"Renamed columns to: {', '.join(column_mappings.values())}")

    # Add 'scheduleAsOf', 'sy', and 'semester' columns
    df["scheduleAsOf"] = datetime.now().strftime("%Y-%m-%d")
    df["sy"] = "SY24"
    df["semester"] = "S1"

    return df


def delete_old_data_from_db(client):
    """
    Deletes old data from the database table.
    """
    query = f"DELETE FROM `{project_id}.{table_id}` WHERE 1=1;"
    query_job = client.query(query)
    query_job.result()
    print("Data deletion completed.")


def convert_to_standard_date(date_str):
    """
    Converts date from MM/DD/YY format to YYYY-MM-DD.
    Assumes all two-digit years from 00-21 refer to 2000-2021, and 22-99 refer to 1922-1999.
    """
    month, day, year = map(int, date_str.split("/"))
    if 0 <= year <= 21:
        year += 2000
    elif 22 <= year <= 99:
        year += 1900
    return f"{year:04d}-{month:02d}-{day:02d}"


def upload_to_big_query(df):
    """
    Uploads processed dataframe to BigQuery.
    """
    schema = [
        {"name": "id", "type": "INTEGER"},
        {"name": "lastName", "type": "STRING"},
        {"name": "firstName", "type": "STRING"},
        {"name": "grade", "type": "INTEGER"},
        {"name": "classCode", "type": "STRING"},
        {"name": "class", "type": "STRING"},
        {"name": "room", "type": "STRING"},
        {"name": "teacher", "type": "STRING"},
        {"name": "period", "type": "STRING"},
        {"name": "yog", "type": "INTEGER"},
        {"name": "scheduleAsOf", "type": "DATE"},
        {"name": "sy", "type": "STRING"},
        {"name": "semester", "type": "STRING"},
    ]

    pandas_gbq.to_gbq(
        df,
        destination_table=table_id,
        project_id=project_id,
        if_exists="append",
        table_schema=schema,
        progress_bar=True,
    )


def archive_source_file(csv_file):
    """
    Moves source CSV to an archive folder with a timestamp.
    """
    current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    new_file_name = f"{archive_name}-{current_datetime}.csv"
    destination_file_path = os.path.join(destination_folder, new_file_name)
    df.to_csv(destination_file_path, index=False)
    os.remove(os.path.join(source_folder, csv_file))
    print(f"File '{csv_file}' has been archived as '{new_file_name}'.")


if __name__ == "__main__":
    print("Starting script...")

    csv_files = [f for f in os.listdir(source_folder) if f.endswith(".csv")]

    if csv_files:
        csv_file = csv_files[0]
        csv_path = os.path.join(source_folder, csv_file)

        df = pd.read_csv(csv_path).dropna(how="all")
        print("Deleted empty rows from the DataFrame.")

        client = bigquery.Client(project=project_id)
        if client:
            df = clean_data(df)
            delete_old_data_from_db(client)
            upload_to_big_query(df)
            archive_source_file(csv_file)
        else:
            print("Failed to initialize BigQuery client.")
    else:
        print(f"No CSV files found in the '{archive_name}' folder.")


# needs to get YOG and upload date for "asOf" and include the school year and semester
#
