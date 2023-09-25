from colorama import Fore
import os
import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from datetime import datetime

# Configuration
PROJECT_ID = "chitechdb"
TABLE_ID = "attendance.half-day-report"
SOURCE_FOLDER = "../dataUploaders/half-day-report"
DESTINATION_FOLDER = "../dataUploaders/archivedFiles"
COLUMN_MAPPINGS = {
    "Student > Name": "name",
    "Date": "date",
    "Student > Grade": "grade",
    "Student > Homeroom": "homeroom",
    "Code": "code",
    "Time": "time",
    "Absent?": "absent",
    "Tardy?": "tardy",
    "Excused?": "excused",
    "PcntAbs": "percentAbs",
    "Other": "other",
}


def rename_columns(df):
    df.rename(columns=COLUMN_MAPPINGS, inplace=True)


def drop_unnecessary_columns(df):
    columns_to_remove = ["time", "absent", "tardy", "excused", "other", "homeroom"]
    return df.drop(columns=columns_to_remove)


def delete_old_data(client, min_date, max_date):
    query = f"""
    DECLARE start_date DATE DEFAULT '{min_date}';
    DECLARE end_date DATE DEFAULT '{max_date}';
    DELETE FROM `chitechdb.{TABLE_ID}`
    WHERE date >= start_date AND date <= end_date AND sy="SY24";
    """
    client.query(query).result()
    print(Fore.BLUE + "Data deletion completed.")


def fetch_student_ids(df, client):
    query = "SELECT name, id FROM `chitechdb.student_info.allStudents`"
    mapping_df = client.query(query).to_dataframe()
    return pd.merge(df, mapping_df, on="name", how="left")


def convert_to_standard_date(date_str):
    month, day, year = map(int, date_str.split("/"))
    if year < 100:
        year += 2000 if year >= 22 else 1900
    return f"{year:04d}-{month:02d}-{day:02d}"


def determine_school_year(date):
    year, month = map(int, date.split("-"))
    return f"SY{year}" if month >= 8 else f"SY{year - 1}"


def determine_semester(date):
    _, month = map(int, date.split("-"))
    return "S1" if month >= 8 else "S2"


def upload_to_bigquery(df):
    schema = [
        {"name": "date", "type": "DATE"},
        {"name": "name", "type": "STRING"},
        {"name": "grade", "type": "STRING"},
        {"name": "code", "type": "STRING"},
        {"name": "percentAbs", "type": "FLOAT"},
        {"name": "id", "type": "INTEGER"},
        {"name": "sy", "type": "STRING"},
        {"name": "semester", "type": "STRING"},
    ]
    pandas_gbq.to_gbq(df, destination_table=TABLE_ID, project_id=PROJECT_ID, if_exists="append", table_schema=schema, progress_bar=True)
    print(Fore.BLUE + "Data uploaded to BigQuery table.")


def move_file_to_destination(csv_file, df):
    current_date = datetime.now().strftime("%Y-%m-%d")
    new_filename = f"half-day-{current_date}.csv"
    destination_path = os.path.join(DESTINATION_FOLDER, new_filename)
    df.to_csv(destination_path, index=False)
    os.remove(csv_file)
    print(f"File '{csv_file}' has been saved as '{new_filename}' and moved to '{DESTINATION_FOLDER}'.")


if __name__ == "__main__":
    csv_files = [f for f in os.listdir(SOURCE_FOLDER) if f.endswith(".csv")]

    if csv_files:
        csv_file = os.path.join(SOURCE_FOLDER, csv_files[0])
        data = pd.read_csv(csv_file)
        print(Fore.GREEN + f"Found file: {csv_files[0]}")

        client = bigquery.Client(project=PROJECT_ID)

        if client:
            print(Fore.BLUE + "BigQuery client initialized.")

            data = data.dropna(how="all")
            rename_columns(data)
            data = drop_unnecessary_columns(data)
            data = fetch_student_ids(data, client)
            data["date"] = data["date"].apply(convert_to_standard_date)
            data["sy"] = data["date"].apply(determine_school_year)
            data["semester"] = data["date"].apply(determine_semester)

            min_date, max_date = data["date"].min(), data["date"].max()
            print(Fore.BLUE + f"Date range: {min_date} to {max_date}")

            delete_old_data(client, min_date, max_date)
            upload_to_bigquery(data)
            move_file_to_destination(csv_file, data)
        else:
            print(Fore.RED + "Failed to initialize BigQuery client.")
    else:
        print(Fore.RED + f"No CSV files found in '{SOURCE_FOLDER}'")
