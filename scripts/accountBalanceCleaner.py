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
    "id", "date", "drop1", "dropLast", "type", "feeName", 
    "drop2", "comment", "drop3", "drop4", "amountDue", 
    "drop5", "drop6", "amountPaid", "drop7", "drop8", "balance"
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

    df.columns = column_names

    current_student_id = None
    for index, row in df.iterrows():
        if "Student ID:" in str(row["id"]):
            current_student_id = row["dropLast"]
        elif pd.isna(row["id"]) and pd.notna(row["date"]):
            df.at[index, "id"] = current_student_id

    drop_columns = [f'drop{i}' for i in range(1, 9)] + ["dropLast"]
    df.drop(columns=drop_columns, inplace=True, errors='ignore')

    df["comment"] = df["comment"].str.strip()
    df["feeName"] = df["feeName"].str.strip()
    df["type"] = df["type"].str.strip()

    df = df[~df["id"].str.contains("Student ID:", na=False)]

    date_pattern = re.compile(r'\d{1,2}/\d{1,2}/\d{2,4}')
    df = df[df["date"].str.match(date_pattern, na=False)]
    df["date"] = df["date"].apply(convert_to_standard_date)


    # Fetch student information from roster
    unique_ids = df["id"].dropna().unique()
    roster_df = fetch_roster_data(unique_ids)

    # Merge roster data to df based on 'id'
    df = pd.merge(df, roster_df, on='id', how='left')

    # Reorder columns
    df = df[["name", "id", "yog", "date", "type", "feeName", "comment", "amountDue", "amountPaid", "balance"]]

    df['amountDue'] = strip_dollar_sign_and_convert(df['amountDue'])
    df['amountPaid'] = strip_dollar_sign_and_convert(df['amountPaid'])
    df['balance'] = strip_dollar_sign_and_convert(df['balance'])


    return df

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

def strip_dollar_sign_and_convert(column):
    return pd.to_numeric(column.str.replace("$", "").str.strip(), errors='coerce')

def delete_all_data_from_table():
    """
    Deletes all data from the specified BigQuery table.
    """
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
        {"name": "date", "type":"DATE"},
        {"name": "type", "type":"STRING"},
        {"name": "feeName", "type":"STRING"},
        {"name": "comment", "type":"STRING"},
        {"name": "amountDue", "type":"FLOAT"},
        {"name": "amountPaid", "type":"FLOAT"},
        {"name": "balance", "type":"FLOAT"},
    ]

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