import os
import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from datetime import datetime
import re

# Constants
project_id = "chitechdb"
table_id = "student_info.emails"
roster_table_id = "student_info.roster"
source_folder = "../dataUploaders/studentEmails"
destination_folder = "../dataUploaders/archivedFiles"
archive_name = "studentEmails"

# Desired column names
column_names = [
    "firstName","lastName","emailAddress","drop1","drop2","orgUnit","drop3","status",
    "drop4","drop5","drop6","drop7","drop8","drop9","drop10","drop11","drop12","drop13",
    "id",
    "drop14","drop15","drop16","drop17","drop18","drop19","drop20","drop21","drop22","drop23",
    "drop24","drop25","drop26","drop27","drop28","drop29","drop30"
]


def clean_data(df):
    print("Cleaning data...")

    # Make a deep copy to ensure we're not working on a view
    df = df.copy()

    # Rename columns
    df.columns = column_names

    # Drop unwanted columns
    df = df[['firstName', 'lastName', 'emailAddress', 'orgUnit', 'status', 'id']]

    # Join the names
    df['name'] = df['lastName'] + ", " + df['firstName']

    # Keep only rows where 'orgUnit' starts with "/Students/Class of"
    df = df[df['orgUnit'].str.startswith("/Students/Class of")]

    # Delete rows where 'status' is "Suspended"
    df = df[df['status'] != 'Suspended']

    # Query the roster table to get a lookup DataFrame
    client = bigquery.Client(project=project_id)
    query = f"SELECT name, id FROM `{roster_table_id}`"
    roster_df = client.query(query).to_dataframe()

    # Left join with the roster DataFrame to fill missing IDs
    df = df.merge(roster_df, on='name', how='left', suffixes=('', '_roster'))

    # Use the ID from the roster, drop the original ID
    df['id'] = df['id_roster']
    df.drop(columns=['id_roster'], inplace=True)  # Drop the temporary 'id_roster' column



    # Delete rows where 'id' is NaN (missing)
    # df.dropna(subset=['id'], inplace=True)


    # Drop unwanted columns (optional, uncomment if needed)
    df = df[['id', 'name', 'emailAddress']]
   
    print("Merging data...")
    return df



def delete_all_data_from_table():
    """
    Deletes all data from the specified BigQuery table.
    """
    print("Deleting old data from database to override...")
    client = bigquery.Client(project=project_id)
    query = f"DELETE FROM `{table_id}` WHERE TRUE"
    client.query(query).result()  # Execute the DELETE query
    print(f"All data from {table_id} has been deleted.")

def archive_source_file(csv_file, df):
    current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    new_file_name = f"{archive_name}-{current_datetime}.csv"
    destination_file_path = os.path.join(destination_folder, new_file_name)
    df.to_csv(destination_file_path, index=False)
    os.remove(os.path.join(source_folder, csv_file))
    print(f"File '{csv_file}' has been archived as '{new_file_name}'.")

def upload_to_big_query(df):
    # Define the schema for the BigQuery table
    schema = [
        {"name": "id", "type":"INTEGER"},
        {"name": "name", "type":"STRING"},
        {"name": "emailAddress", "type":"STRING"}
    ]
    print("Uploading data to BigQuery...")
    # Upload the dataframe
    pandas_gbq.to_gbq(df, destination_table=table_id, project_id=project_id,
                      if_exists="append", progress_bar=True, table_schema=schema)


if __name__ == "__main__":
    print("Starting script...")

    csv_files = [f for f in os.listdir(source_folder) if f.endswith('.csv')]
    
    if csv_files:
        csv_file = csv_files[0]
        csv_path = os.path.join(source_folder, csv_file)

        df = pd.read_csv(csv_path).dropna(how="all")
        print("Deleted empty rows from the DataFrame.")
        
        df = clean_data(df)
        
        delete_all_data_from_table()  # Call this function before uploading new data
        upload_to_big_query(df)
        archive_source_file(csv_file, df)
    else:
        print(f"No CSV files found in the '{archive_name}' folder.")