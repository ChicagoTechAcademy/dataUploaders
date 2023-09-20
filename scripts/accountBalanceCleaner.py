import os
import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from datetime import datetime
import re

# Constants
project_id = "chitechdb"
table_id = "student_info.accountBalances"
roster_table_id = "student_info.roster"
source_folder = "../dataUploaders/accountBalances"
destination_folder = "../dataUploaders/archivedFiles"
archive_name = "accountBalances"

# Desired column names
column_names = [
    "id", "balance", "drop1", "drop2", "drop3","drop4","drop5","drop6","drop7","drop8","drop9","drop10","drop11","drop12","drop13","drop14","drop15",
]

def fetch_roster_data(ids):
    """
    Queries BigQuery for the name and YOG based on a list of IDs.
    Returns a DataFrame.
    """
    client = bigquery.Client(project=project_id)
    query = f"SELECT id, name, yog FROM `{roster_table_id}` WHERE id IN {tuple(ids)}"
    roster_df = client.query(query).to_dataframe()
    return roster_df

def clean_data(df):
    print("Cleaning data...")

    # Rename columns
    df.columns = column_names

    df = df.reset_index(drop=True)  # Reset index
    current_id = None  # This will store the value from "drop2" whenever we find "Student ID:"

    for index in range(len(df)):
        if df.loc[index, 'id'] == 'Student ID:':
            current_id = df.loc[index, 'drop2']
        elif current_id is not None:
            df.loc[index, 'id'] = current_id
    
    # Drop unwanted columns
    df = df[['id', 'balance']]

    # Filter rows based on 'bal'
    df = df[df['balance'].str.startswith('This is a current', na=False)]

    # Remove 'This is a current balance' from 'bal'
    # This is a current statement of your account.  The total amount due is  $ 150.00  and is payable upon the indicated date.  
    df['balance'] = df['balance'].str.replace('This is a current statement of your account.  The total amount due is  $ ', '')
    df['balance'] = df['balance'].str.replace('  and is payable upon the indicated date.  ', '')

    # Convert 'bal' to float
    df['balance'] = df['balance'].astype(float)

    print("Getting names and YOG from data base...")
    # # Fetch student information from roster
    unique_ids = df["id"].dropna().unique().astype(int)  # Convert IDs to integers
    roster_df = fetch_roster_data(unique_ids)

    # # Merge roster data to df based on 'id'
    df = pd.merge(df, roster_df, on='id', how='left')

    # # Reorder columns
    df = df[["name", "id", "yog", "balance"]]

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
        {"name": "name", "type":"STRING"},
        {"name": "id", "type":"INTEGER"},
        {"name": "yog", "type":"INTEGER"},
        {"name": "balance", "type":"FLOAT"},
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