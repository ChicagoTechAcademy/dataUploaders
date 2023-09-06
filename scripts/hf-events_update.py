import os
import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from datetime import datetime

# Define your GCP project ID and BigQuery dataset ID
project_id = "chitechdb"
dataset_id = "behavior"
table_id = "behavior.HF-Events"

# Specify the paths
source_folder = "../dataUploaders/HF-Events"
destination_folder = "../dataUploaders/archivedFiles"

# Define the column name mappings
column_mappings = {
    "date" : "date",
    "student_id" : "id",
    "student_name" : "name",
    "grade" : "grade",
    "race" : "race",
    "ethnicity" : "ethnicity",
    "gender" : "gender",
    "event_group" : "event_group",
    "event_type" : "event_type",
    "class" : "class",
    "teacher" : "teacher",
    "is_resolved" : "is_resolved"
}

def cleanData(df):
    df = df.drop(df.columns[13], axis=1)
    df = df.drop(df.columns[12], axis=1)

    # Rename the columns that need renaming
    for old_col, new_col in column_mappings.items():
        if old_col in df.columns and new_col not in df.columns:
            df.rename(columns={old_col: new_col}, inplace=True)
    return df

def deleteOldDataFromDB(client):
    query = f"""
    -- Delete data between the specified dates
    DELETE FROM `chitechdb.{table_id}`
    WHERE 1=1;
    """.format(
        project_id, dataset_id
    )

    query_job = client.query(query)
    query_job.result()
    print("Data deletion completed.")


# Convert the "date" column to "YYYY-MM-DD" format
def convert_to_standard_date(date_str):
    month, day, year = map(int, date_str.split("/"))
    if year < 100:
        if year >= 22:
            year += 2000
        else:
            year += 1900
    return f"{year:04d}-{month:02d}-{day:02d}"


# Calculate the "sy" column based on the year and month
def get_sy(date):
    year, month, _ = map(int, date.split("-"))
    if (year == 2022 and month >= 8) or (year == 2023 and month <= 7):
        return "SY23"
    elif (year == 2023 and month >= 8) or (year == 2024 and month <= 7):
        return "SY24"
    return str(year)  # Convert year to string

# Send the DataFrame to BigQuery
def uploadToBigQuery(df):
    pandas_gbq.to_gbq(
        df,
        destination_table=table_id,
        project_id=project_id,
        if_exists="append",
        table_schema=[
            {"name" : "date","type": "DATE"},
            {"name" : "id","type": "INTEGER"},
            {"name" : "name","type": "STRING"},
            {"name" : "grade","type": "INTEGER"},
            {"name" : "race","type": "STRING"},
            {"name" : "ethnicity","type": "STRING"},
            {"name" : "gender","type": "STRING"},
            {"name" : "event_group","type": "STRING"},
            {"name" : "event_type","type": "STRING"},
            {"name" : "class","type": "STRING"},
            {"name" : "teacher","type": "STRING"},
            {"name" : "is_resolved","type": "STRING"},
        ],
        progress_bar=True,
    )


# Function to get today's date and move the file to done&uploaded folder
def moveSourceFileToUsedFolder():
    # Generate the new file name with the date
    current_date = datetime.now().strftime("%Y-%m-%d")
    new_file_name = f"HF-event-{current_date}.csv"
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
        print("BigQuery client initialized.")
        # Remove empty rows from the DataFrame
        df = df.dropna(how="all")

        

        df = cleanData(df)

        

        

        # df["date"] = df["date"].apply(convert_to_standard_date)

        # Delete the old data from the BigQuery table
        deleteOldDataFromDB(client)

        # Upload the DataFrame to BigQuery
        uploadToBigQuery(df)

        print("Data uploaded to BigQuery table.")

        moveSourceFileToUsedFolder()
    else:
        print("Failed to initialize BigQuery client.")

else:
    print("No CSV files found in the 'at-report' folder.")
