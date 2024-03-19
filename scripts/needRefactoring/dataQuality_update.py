import os
import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from datetime import datetime
from colorama import Fore


# Define your GCP project ID and BigQuery dataset ID
project_id = "chitechdb"
dataset_id = "logistics"
table_id = "dataQuality"

# Specify the paths
source_folder = "../dataUploaders/dataQuality"
destination_folder = "../dataUploaders/archivedFiles"
cleaned_file_name = "cleaned_dataQuality"

# Prompt the user for the week number
week_number = input("Enter the week number (e.g., W01): ")

# Prompt the user for the number of errors, total percent errors, and grade
num_errors = int(input("Enter the number of errors: "))
total_percent_errors = float(input("Enter the total percent errors: "))
grade = input("Enter the grade: ")

# Define the column name mappings
column_mappings = {
    "Area": "qualityArea",
    "Measure": "measure",
    "Weight": "weight",
    "Errors": "errorCount",
    "% Error": "percentError",
    "Grade": "grade",
}

def renameColumns(df):
    # Rename the columns that need renaming
    for old_col, new_col in column_mappings.items():
        if old_col in df.columns and new_col not in df.columns:
            df.rename(columns={old_col: new_col}, inplace=True)

# Function to add the "week" column with the specified week_number
def addWeekColumn(df, week_number):
    df["week"] = week_number

# Function to remove "%" and convert to float for percentError
def cleanPercentError(df):
    df["percentError"] = df["percentError"].str.rstrip('%').astype(float)

# Function to clean "weight" column by removing "%" and convert to integers
def cleanAndConvertWeight(df):
    df["weight"] = df["weight"].str.rstrip('%').astype(int)

# Function to add rows with user-specified values
def addCustomRows(df, num_errors, total_percent_errors, grade):
    custom_rows = {
        "qualityArea": "total",
        "measure": ["totalErrors", "totalPercentErrors", "dataQualityGrade"],
        "weight": [None, None, None],
        "errorCount": [num_errors, num_errors, None],
        "percentError": [total_percent_errors, total_percent_errors, None],
        "grade": [grade, grade, grade],
        "week": [week_number, week_number, week_number],
    }
    
    custom_df = pd.DataFrame(custom_rows)
    df = pd.concat([df, custom_df], ignore_index=True)
    
    return df

# Send the DataFrame to BigQuery
def uploadToBigQuery(df):
    client = bigquery.Client(project=project_id)
    
    # Define schema for the BigQuery table
    schema = [
        bigquery.SchemaField("qualityArea", "STRING"),
        bigquery.SchemaField("measure", "STRING"),
        bigquery.SchemaField("weight", "FLOAT"),
        bigquery.SchemaField("errorCount", "INTEGER"),
        bigquery.SchemaField("percentError", "FLOAT"),
        bigquery.SchemaField("grade", "STRING"),
        bigquery.SchemaField("week", "STRING"),
    ]

    # Create a BigQuery table
    table_ref = client.dataset(dataset_id).table(table_id)
    table = bigquery.Table(table_ref, schema=schema)
    client.create_table(table, exists_ok=True)

    # Load data into the BigQuery table
    job_config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_APPEND")
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Wait for the job to complete

# Function to get today's date and move the file to done&uploaded folder
def moveSourceFileToUsedFolder():
    # Generate the new file name with the date
    current_date = datetime.now().strftime("%Y-%m-%d")
    new_file_name = f"{cleaned_file_name}-{current_date}.csv"
    destination_file_path = os.path.join(destination_folder, new_file_name)

    # Save the CSV file with the new name to the destination folder using pandas
    df.to_csv(destination_file_path, index=False)

    # Delete the source file
    os.remove(source_file_path)

    print(Fore.GREEN +
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

    # Add the "week" column with the specified week_number
    addWeekColumn(df, week_number)

    # Initialize the BigQuery client
    client = bigquery.Client(project=project_id)

    if client:
        print(Fore.BLUE +"BigQuery client initialized.")
        # Remove empty rows from the DataFrame
        df = df.dropna(how="all")

        renameColumns(df)
        cleanPercentError(df)  # Clean percentError column
        cleanAndConvertWeight(df)  # Clean and convert Weight column

        # Add custom rows with user-specified values
        df = addCustomRows(df, num_errors, total_percent_errors, grade)

        # Upload the DataFrame to BigQuery
        uploadToBigQuery(df)

        print(Fore.BLUE +"Data uploaded to BigQuery table.")

        moveSourceFileToUsedFolder()
    else:
        print(Fore.RED + "Failed to initialize BigQuery client.")

else:
    print(Fore.RED + f"No CSV files found in the {source_folder} folder.")
