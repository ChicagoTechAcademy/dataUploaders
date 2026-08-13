from _dataManager import *


from colorama import init, Fore
init(autoreset=True)


scriptName = "roster_update"


# Constants
project_id = "chitechdb"
dataset_id = "studentInfo"
table_id = "roster"
source_folder = f"../dataUploaders/{table_id}"

tableSchema = [
    {"name": "entryID", "type": "INTEGER"},
    {"name": "id", "type": "INTEGER"},
    {"name": "name", "type": "STRING"},
    {"name": "yog", "type": "INTEGER"},
    {"name": "homeroom", "type": "STRING"},
    
]


# Define the column name mappings
column_mappings = {
    "Name": "name",
    "Student ID": "id",
    "YOG": "yog",
    "Homeroom": "homeroom",
}

# Get the entry ID from the user
def getEntryID():
    # entryID = input(Fore.CYAN + "Enter the entry ID for this roster YYYYMMDDX: ")

    # find todays date, and reformat it as YYYYMMDD,
    entryID = pd.Timestamp.now().strftime("%Y%m%d")

    # add 1 at the end
    entryID = entryID + "1"

    print(Fore.CYAN + f"The EntryID is {entryID}.")

    return int(entryID)


def cleanData(df):
    entryID = getEntryID()

    # The data has 22 columns. I only need 3 of them. Drop the rest.
    df = df[list(column_mappings.keys())]   # Keep only the columns we need
    df = df.rename(columns=column_mappings)  # Rename the columns
    
    df['entryID'] = int(entryID)  # Add the entryID column with the user input value

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

    # Upload the data to BigQuery
    uploadToBigQuery(cleanedDataFrame, tableSchema, project_id, dataset_id, table_id)

   

    # Archive the source file
    archiveSourceFile(cleanedDataFrame, csv_file, source_folder, table_id)

    print(Fore.GREEN + f"{scriptName} data pull complete.")


doWork()
