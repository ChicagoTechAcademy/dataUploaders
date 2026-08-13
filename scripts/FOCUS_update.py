from _dataManager import *
import os


scriptName = "FOCUS_update"


# Constants
project_id = "chitechdb"
dataset_id = "attendance"
table_id = "FOCUS"
source_folder = f"../dataUploaders/{table_id}"

schema = [
    {"name": "entryID", "type": "INTEGER"},
    {"name": "id", "type": "INTEGER"},
    {"name": "name", "type": "STRING"},
    {"name": "status", "type": "STRING"},
    {"name": "period", "type": "STRING"},
    {"name": "date", "type": "DATE"},
    {"name": "time", "type": "TIME"},
]

# Mapping of original column names to new names
column_mappings = {
    "Swipe_Time": "swipe_time",
    "Student_ID": "id",
    "Division": "division",
    "Student_Name": "name",
    "Status": "status",
    "Period_Cause": "period",
    "Row_Number": "row_number",
    "Textbox6": "student_tardy_count",
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

    # Rename columns based on predefined mappings
    df.rename(columns=column_mappings, inplace=True, errors="raise")

    # Convert swipe_time to date and time columns. Time is given as 24-hour format HH:MM:SS, and should stay that way.
    # Example: "2023-08-30 08:15:00"
    dt = pd.to_datetime(df["swipe_time"].astype(str).str.strip(), errors="coerce")
    df["date"] = dt.dt.date
    df["time"] = dt.dt.time

    # Drop unnecessary columns
    df.drop(columns=["swipe_time", "division","row_number", "student_tardy_count"], inplace=True, errors="ignore")

    # Convert id, row_number, and student_tardy_count to integers
    df["id"] = df["id"].astype(int)

    # Convert name, status, and period to strings
    df["name"] = df["name"].astype(str)
    df["status"] = df["status"].astype(str)
    df["period"] = df["period"].astype(str)

    # Convert date and time to date and time types
    # Time should be given as 24-hour format HH:MM:SS
    # df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    # df["time"] = pd.to_datetime(df["time"], format="%H:%M:%S", errors="coerce").dt.time

    # add entryID
    df["entryID"] = getEntryID()

    # reorder columns
    df = df[["entryID", "id", "name", "status", "period", "date", "time"]]

    return df


def find_csv_file(folder_path):
    """
    Finds the first .csv file in the given folder path.
    Returns the file path if found, otherwise returns None.
    """
    for file in os.listdir(folder_path):
        if file.endswith(".csv"):
            return os.path.join(folder_path, file)
    return None


def prepareCSVFile(source_folder):

    csv_path = find_csv_file(source_folder)

    """
    Removes the first three lines from a CSV file.
    """
    with open(csv_path, "r") as file:
        data = file.readlines()[3:]

    with open(csv_path, "w") as file:
        file.writelines(data)


def doWork():
    """
    Main function for the script.
    """
    print(f"Starting {scriptName} script...")

    prepareCSVFile(source_folder)

    # Read in the first .csv file found
    csv_file, rawDataFrame = readCSV(source_folder)

    # Clean the data
    cleanedDataFrame = cleanData(rawDataFrame)

    deleteAllDataFromTable(project_id, dataset_id, table_id)

    # Upload the data to BigQuery
    uploadToBigQuery(cleanedDataFrame, schema, project_id, dataset_id, table_id)

    # Archive the source file
    archiveSourceFile(cleanedDataFrame, csv_file, source_folder, table_id)

    print(Fore.GREEN + f"{scriptName} data pull complete.")


doWork()
