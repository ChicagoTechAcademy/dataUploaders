import datetime

from colorama import Fore
from _dataManager import *



scriptName = "schedule_update"


# Constants
project_id = "chitechdb"
dataset_id = "schedules"
table_id = "studentSchedules"
source_folder = f"../dataUploaders/{table_id}"

tableSchema = [
    {"name": "entryID", "type": "INTEGER"},
    {"name": "id", "type": "INTEGER"},
    {"name": "classCode", "type": "STRING"},
    {"name": "class", "type": "STRING"},
    {"name": "teacher", "type": "STRING"},
    {"name": "period", "type": "STRING"},
    {"name": "semester", "type": "STRING"},
    {"name": "sy", "type": "STRING"},
    ]

column_mappings = {
    "Student ID": "id",
    "Class": "classCode",
    "Description": "class",
    "Name": "teacher",
    "Schedule": "period",
}

# Get the entry ID from the user
def getEntryID():
    # entryID = input(Fore.CYAN + "Enter the entry ID for this roster YYYYMMDDX: ")

    # find todays date, and reformat it as YYYYMMDD,
    entryID = pd.Timestamp.now().strftime("%Y%m%d")

    print(Fore.CYAN + f"The EntryID is {entryID}.")

    return int(entryID)

def getWeek():
    return input(Fore.CYAN + "Enter the week for this roster (W01, W02, ...): ")


def cleanData(df):
    """
    Processes the dataframe for cleanup tasks.
    """

    print("Cleaning data...")

    # Drop unnecessary columns
    df.drop(
        columns=[
            "LastName",
            "FirstName",
            "MiddleName",
            "SpecialEdStus",
            "Grade",
            "Homeroom",
            "Inclusion?",
            "SecType",
            "Clssrm",
            "Total",
            "Max",
        ],
        inplace=True,
        errors="ignore",
    )

    # Rename columns based on predefined mappings
    df.rename(columns=column_mappings, inplace=True, errors="raise")

    # Add 'entryID' column
    df["entryID"] = getEntryID()

    # Add 'sy' & 'semester' columns
    df["sy"] = "SY26"
    df["semester"] = df["classCode"].apply(lambda x: "Other" if x[6] == "N" else "S1" if x[5] == "1" else "S2" if x[5] == "2" else None)

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

    deleteAllDataFromTable(project_id, dataset_id, table_id)

    # Upload the data to BigQuery
    uploadToBigQuery(cleanedDataFrame, tableSchema, project_id, dataset_id, table_id)

    # Archive the source file
    archiveSourceFile(cleanedDataFrame, csv_file, source_folder, table_id)


    print(Fore.GREEN + f"{scriptName} data pull complete.")


doWork()

