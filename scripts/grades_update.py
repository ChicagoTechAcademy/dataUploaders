from colorama import Fore
import os
import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from datetime import datetime, date
import re

# Constants
project_id = "chitechdb"
table_id = "academics.grades"
roster_table_id = "student_info.roster"
source_folder = "../dataUploaders/grades"
destination_folder = "../dataUploaders/archivedFiles"
archive_name = "grades"

# Desired column names
column_names = ["yog", "name","drop1","drop2","id", "course", "drop3","teacher", "drop4","averageAndLetter"]

def clean_data(df):
    print("Cleaning data...")

    # Rename columns
    df.columns = column_names
    
    checkpoint = input("Enter the Checkpoint in 'Q# CP#' format: ")

    # Delete rows where 'id' is not a number
    df['id'] = pd.to_numeric(df['id'], errors='coerce')
    df = df.dropna(subset=['id'])

   

    # Split 'averageAndLetter' into two columns: 'gradePercent' and 'letterGrade'
    df[['gradePercent', 'letterGrade']] = df['averageAndLetter'].str.split(expand=True)
    # Drop the original 'averageAndLetter' column
    df.drop(columns=['averageAndLetter'], inplace=True)

    # Add new columns
    today = date.today()
    df['checkpoint'] = "SY24: " + checkpoint
    df['date'] = today.strftime("%Y-%m-%d")
    df['sy'] = "SY24"

     # Convert 'id' column to integer type
    df['id'] = df['id'].astype(int)
    df['yog'] = df['yog'].astype(int)
    df['gradePercent'] = df['gradePercent'].astype(float)

    # Reorder columns
    df = df[["checkpoint", "date", "id", "name", "yog", "course", "teacher", "gradePercent", "letterGrade", "sy"]]

    print("Merging data...")
    return df


def fetch_roster_data(ids):
    """
    Queries BigQuery for the name and YOG based on a list of IDs.
    Returns a DataFrame.
    """
    client = bigquery.Client(project=project_id)
    query = f"SELECT id, name, yog FROM `{roster_table_id}` WHERE id IN {tuple(ids)}"
    roster_df = client.query(query).to_dataframe()
    return roster_df


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

    print("Uploading data to BigQuery...")
    # Upload the dataframe
    pandas_gbq.to_gbq(df, destination_table=table_id, project_id=project_id,
                      if_exists="append", progress_bar=True, table_schema=schema)


if __name__ == "__main__":
    print("Starting script...")
    print(Fore.RED + "WARNING:")
    print(Fore.WHITE + "YOU MUST MANUALLY SCROLL TO THE BOTTOM OF THE CSV FILE AND DELETE THE LAST ROW.")
    print(Fore.WHITE + "THEN MOVE THE LAST COLUMN OVER TO SIT BELOW THE OTHER GRADE COLUMNS.")
    csv_files = [f for f in os.listdir(source_folder) if f.endswith('.csv')]
    
    if csv_files:
        csv_file = csv_files[0]
        csv_path = os.path.join(source_folder, csv_file)

        df = pd.read_csv(csv_path).dropna(how="all")
        print("Deleted empty rows from the DataFrame.")
        
        df = clean_data(df)
        
        # delete_all_data_from_table()  # Call this function before uploading new data
        upload_to_big_query(df)
        archive_source_file(csv_file, df)
    else:
        print(f"No CSV files found in the '{archive_name}' folder.")