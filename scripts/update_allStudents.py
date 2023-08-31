import os
import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from datetime import datetime

# Define your GCP project ID and BigQuery dataset ID
project_id = "chitechdb"
dataset_id = "student_info"
table_id = "attendance.allStudents"

# Specify the paths
source_folder = "../dataUploaders/allStudents"
destination_folder = "../dataUploaders/archivedFiles"

# Define the column name mappings
# column_mappings = {
#     "Student > Name": "name",
#     "Student > Student ID": "id",
#     "Date": "date",
#     "Code": "code",
#     "Master > Class": "course",
#     "Master > Description": "class",
#     "Period": "period",
#     "Tardy?": "tardy",
#     "Absent?": "absent",
#     "Student > YOG": "yog",
# }


def cleanData(df):
    print("Cleaning data...")


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


# Send the DataFrame to BigQuery
def uploadToBigQuery(df):
    pandas_gbq.to_gbq(
        df,
        destination_table=table_id,
        project_id=project_id,
        if_exists="append",
        table_schema=[
            {"name": "name", "type": "STRING"},
            {"name": "id", "type": "INTEGER"},
            {"name": "DOB", "type": "DATE"},
            {"name": "enrollment", "type": "STRING"},
            {"name": "yog", "type": "INTEGER"},
            {"name": "graduationDate", "type": "DATE"},
        ],
        progress_bar=True,
    )

# Function to get today's date and move the file to done&uploaded folder
def moveSourceFileToUsedFolder():
    # Generate the new file name with the date
    current_date = datetime.now().strftime("%Y-%m-%d")
    new_file_name = f"at-report-{current_date}.csv"
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

        cleanData(df)

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
