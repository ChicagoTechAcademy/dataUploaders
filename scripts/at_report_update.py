from _dataManager import *


from colorama import init, Fore

init(autoreset=True)

# Constants
scriptName = "at_report_update"


# Constants
project_id = "chitechdb"
dataset_id = "attendance"
table_id = "at-report"
source_folder = f"../dataUploaders/{table_id}"

tableSchema = [
    {"name": "entryID", "type": "INTEGER"},
    {"name": "name", "type": "STRING"},
    {"name": "id", "type": "INTEGER"},
    {"name": "date", "type": "DATE"},
    {"name": "code", "type": "STRING"},
    {"name": "course", "type": "STRING"},
    {"name": "class", "type": "STRING"},
    {"name": "period", "type": "STRING"},
    {"name": "sy", "type": "STRING"},
    {"name": "semester", "type": "STRING"},
]


# Define the column name mappings
column_mappings = {
    "Student > Name": "name",
    "Student > Student ID": "id",
    "Date": "date",
    "Code": "code",
    "Master > Class": "course",
    "Master > Description": "class",
    "Period": "period",
    "Tardy?": "tardy",
    "Absent?": "absent",
    "Student > YOG": "yog",
    "entryID": "entryID",
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
    Cleans, processes and renames columns.
    """
    print(Fore.RESET + "Processing data...")

    # Create entryID
    df["entryID"] = getEntryID()

    # Rename columns
    df.rename(columns=column_mappings, inplace=True)

    # drop columns yog tardy, absent
    df.drop(columns=["yog", "tardy", "absent"], inplace=True)

    # Convert date and derive related columns
    df["date"] = df["date"].apply(convertToStandardDate)
    df["sy"] = df["date"].apply(getSchoolYear)
    df["semester"] = df["date"].apply(getSemester)

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

    # purge the table
    deleteDataBetweenDates(
        project_id,
        dataset_id,
        table_id,
        cleanedDataFrame["date"].min(),
        cleanedDataFrame["date"].max(),
    )

    # Upload the data to BigQuery
    uploadToBigQuery(cleanedDataFrame, tableSchema, project_id, dataset_id, table_id)

    # Archive the source file
    archiveSourceFile(cleanedDataFrame, csv_file, source_folder, table_id)

    print(Fore.GREEN + f"{scriptName} data pull complete.")


doWork()
