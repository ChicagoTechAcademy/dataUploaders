from colorama import Fore
import os
import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from datetime import datetime

# Define your GCP project ID and BigQuery dataset ID
project_id = "chitechdb"
dataset_id = "attendance"
table_id = "attendance.half-day-report"

# Specify the paths
source_folder = "../dataUploaders/half-day-report"
destination_folder = "../dataUploaders/archivedFiles"


# Initialize the BigQuery client
client = bigquery.Client(project=project_id)

# Define the column name mappings
column_mappings = {
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


def renameColumns(df):
    # Rename the columns that need renaming
    for old_col, new_col in column_mappings.items():
        if old_col in df.columns and new_col not in df.columns:
            df.rename(columns={old_col: new_col}, inplace=True)

def dropUnnecessaryColumns(df):
    # Drop columns that are not needed
    columns_to_remove = [
        "time",
        "absent",
        "tardy",
        "excused",
        "other",
        "homeroom",
    ]
    df = df.drop(columns=columns_to_remove)

    return df


def deleteOldDataFromDB(client, min_date, max_date):
    # Construct the SQL query
    query = f"""
    DECLARE start_date DATE DEFAULT '{min_date}';
    DECLARE end_date DATE DEFAULT '{max_date}';

    -- Delete data between the specified dates
    DELETE FROM `chitechdb.{table_id}`
    WHERE date >= start_date AND date <= end_date AND sy="SY24";
    """.format(
        project_id, dataset_id
    )

    # Execute the query
    query_job = client.query(query)
    query_job.result()

    print(Fore.BLUE +"Data deletion completed.")


def getStudentIDs(df):
    # Query student IDs based on names from the students table
    name_id_query = """
    SELECT name, id FROM `chitechdb.student_info.allStudents`
    """
    name_id_df = client.query(name_id_query).to_dataframe()

    # Merge the name-id mapping with the original DataFrame
    df = pd.merge(df, name_id_df, on="name", how="left")

    return df


# Convert the "date" column to "YYYY-MM-DD" format
def convert_to_standard_date(date_str):
    month, day, year = map(int, date_str.split("/"))
    if year < 100:
        if year >= 22:
            year += 2000
        else:
            year += 1900
    return f"{year:04d}-{month:02d}-{day:02d}"


# Check and add the "sy" column if it doesn't exist
def get_sy(date):
    year, month, _ = map(int, date.split("-"))
    if (year == 2022 and month >= 8) or (year == 2023 and month <= 7):
        return "SY23"
    elif (year == 2023 and month >= 8) or (year == 2024 and month <= 7):
        return "SY24"
    return str(year)  # Convert year to string


# Calculate the "semester" column based on the month
def get_semester(date):
    _, month, _ = map(int, date.split("-"))
    if month >= 8:
        return "S1"
    elif month >= 1:
        return "S2"
    return ""


# Upload the DataFrame to BigQuery
def uploadToBigQuery(df):
    pandas_gbq.to_gbq(
        df,
        destination_table=table_id,
        project_id=project_id,
        if_exists="append",
        table_schema=[
            {"name": "date", "type": "DATE"},
            {"name": "name", "type": "STRING"},
            {"name": "grade", "type": "STRING"},
            {"name": "code", "type": "STRING"},
            {"name": "percentAbs", "type": "FLOAT"},
            {"name": "id", "type": "INTEGER"},
            {"name": "sy", "type": "STRING"},
            {"name": "semester", "type": "STRING"},
        ],
        progress_bar=True,
    )
    print(Fore.BLUE +"Data uploaded to BigQuery table.")


# Function to get today's date and move the file to done&uploaded folder
def moveSourceFileToUsedFolder():
    # Generate the new file name with the date
    current_date = datetime.now().strftime("%Y-%m-%d")
    new_file_name = f"half-day-{current_date}.csv"
    destination_file_path = os.path.join(destination_folder, new_file_name)

    # Save the CSV file with the new name to the destination folder using pandas
    df.to_csv(destination_file_path, index=False)

    # Delete the source file
    os.remove(source_file_path)

    print(
        f"File '{csv_file}' has been saved as '{new_file_name}' and moved to '{destination_folder}'."
    )


# List all files in the source folder
file_list = os.listdir(source_folder)

# Check if there is a .csv file in the folder
csv_files = [file for file in file_list if file.endswith(".csv")]

if csv_files:
    csv_file = csv_files[0]  # Get the first (and only) CSV file in the list
    source_file_path = os.path.join(source_folder, csv_file)

    # Read the CSV file using pandas
    df = pd.read_csv(source_file_path)
    print(f"Found file: {csv_file}:")
    # Initialize the BigQuery client
    client = bigquery.Client(project=project_id)

    if client:
        print(Fore.BLUE +"BigQuery client initialized.")
        # Remove empty rows from the DataFrame
        df = df.dropna(how="all")

        renameColumns(df)

        df = dropUnnecessaryColumns(df)

        df = getStudentIDs(df)

        df["date"] = df["date"].apply(convert_to_standard_date)

        if "sy" not in df.columns:
            df["sy"] = df["date"].apply(get_sy)

        # Delete rows where "sy" is not "SY24"
        f = df[df["sy"] == "SY24"]

        if "semester" not in df.columns:
            df["semester"] = df["date"].apply(get_semester)

        # Fetch the min and max dates from the DataFrame
        min_date = df["date"].min()
        max_date = df["date"].max()
        print(f"Min date: {min_date}" + " - " + f"Max date: {max_date}")

        # Delete the old data from the BigQuery table
        deleteOldDataFromDB(client, min_date, max_date)

        # Upload the DataFrame to BigQuery
        uploadToBigQuery(df)

        moveSourceFileToUsedFolder()

    else:
        print(Fore.RED +"Failed to initialize BigQuery client.")

else:
    print(Fore.RED +"No CSV files found in the 'at-report' folder.")
