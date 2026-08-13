import pandas as pd

from _dataManager import (
    readCSV,
    uploadToBigQuery,
    deleteAllDataFromTable,
    archiveSourceFile,
    convertToStandardDate,
)
from google.cloud import bigquery

from colorama import init, Fore
init(autoreset=True)

scriptName = "studentEmail_update"

# Constants
project_id = "chitechdb"
dataset_id = "studentInfo"
table_id = "studentEmail"
source_folder = f"../dataUploaders/{table_id}"

tableSchema = [
    {"name": "entryID", "type": "INTEGER"},
    {"name": "id", "type": "INTEGER"},
    {"name": "emailAddress", "type": "STRING"},
    
]


# Define the column name mappings
column_mappings = {
    "entryID": "entryID",
    "Student ID": "id",
    "Email Address": "emailAddress"
}

# Desired column names
column_names = [
    "firstName",
    "lastName",
    "emailAddress",
    "entryID",
    "drop",
    "orgUnit",
    "drop",
    "status",
    "drop",
    "drop",
    "drop",
    "drop",
    "drop",
    "drop",
    "drop",
    "drop",
    "drop",
    "drop",
    "id",
    "drop",
    "drop",
    "drop",
    "drop",
    "drop",
    "drop",
    "drop",
    "drop",
    "drop",
    "drop",
    "drop",
    "drop",
    "drop",
    "drop",
    "drop",
    "drop",
    "drop",
    "drop",
    "drop",
    "drop",
    "drop",
    "drop"
]

# Get the entry ID from the user
def getEntryID():
    # find todays date, and reformat it as YYYYMMDD,
    entryID = pd.Timestamp.now().strftime("%Y%m%d")

    print(Fore.CYAN + f"The EntryID is {entryID}.")

    return int(entryID)

def cleanData(df):

    # Clear the entryID column
    df["entryID"] = ''

    # Add the entryID column
    df["entryID"] = getEntryID()

    # rename the top row
    df.columns = column_names

    # drop all columns called "drop"
    df = df.loc[:, ~df.columns.str.contains("drop")]

    # reset the index
    df.reset_index(drop=True, inplace=True)

    # delete all rows where the status isn't "Active"
    df = df[df["status"] == "Active"]

    # delete all rows where the orgUnit doesn't start with "/Students/Class of"
    df = df[df["orgUnit"].str.startswith("/Students/Class of")]

    # drop the orgUnit column
    df.drop(columns=["orgUnit"], inplace=True)

    # drop the status column
    df.drop(columns=["status"], inplace=True)

    # get the name
    df["name"] = df["lastName"] + ", " + df["firstName"]

    # Query the roster table to get a lookup DataFrame
    client = bigquery.Client(project=project_id)
    query = f"SELECT name, id FROM `{dataset_id}`.roster"
    roster_df = client.query(query).to_dataframe()

    # Left join with the roster DataFrame to fill missing IDs
    df = df.merge(roster_df, on="name", how="left", suffixes=("", "_roster"))

    # Use the ID from the roster, drop the original ID
    df["id"] = df["id_roster"]
    df.drop(
        columns=["id_roster"], inplace=True
    )  # Drop the temporary 'id_roster' column

    # Delete rows where 'id' is NaN (missing)
    # df.dropna(subset=['id'], inplace=True)

    # Drop unwanted columns (optional, uncomment if needed)
    df = df[["id", "emailAddress"]]

    return df


def doWork():
    """
    Main function for the script.
    """
    print(Fore.YELLOW + f"Starting {scriptName} script...")

    # Read in the first .csv file found
    csv_file, rawDataFrame = readCSV(source_folder)

    # Clean the data
    cleanedDataFrame = cleanData(rawDataFrame)

    # delete the data that's in the table
    # deleteAllDataFromTable(project_id, dataset_id, table_id)

    # Upload the data to BigQuery
    # uploadToBigQuery(cleanedDataFrame, tableSchema, project_id, dataset_id, table_id)
   
    # Archive the source file
    archiveSourceFile(cleanedDataFrame, csv_file, source_folder, table_id)

    print(Fore.GREEN + f"{scriptName} data pull complete.")


doWork()


