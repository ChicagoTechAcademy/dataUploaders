from _dataManager import *
from colorama import init, Fore, Style
init(autoreset=True)


scriptName = "medicalCompliance"


# Constants
project_id = "chitechdb"
dataset_id = "studentInfo"
table_id = "medicalCompliance"
source_folder = f"../dataUploaders/{table_id}"

tableSchema = [
    {"name": "entryID", "type": "INTEGER"},
    {"name": "id", "type": "INTEGER"},
    {"name": "name", "type": "STRING"},
    {"name": "reason", "type": "STRING"},
]

# Get the entry ID
def getEntryID():

    # find todays date, and reformat it as YYYYMMDD,
    entryID = pd.Timestamp.now().strftime("%Y%m%d")

    print(Fore.CYAN + f"The EntryID is {entryID}.")

    return int(entryID)

def clean_data(df):
    """
    Cleans and processes data.
    """
    print("Cleaning data...")

    entryID = getEntryID()

    # Drop the "School" column
    df = df.drop(columns=["School"])

    # Extract student ID and reason from the "Notes" column
    df["id"] = df["Notes"].str.extract(r"Student # (\d+)").astype("Int64")
    # Extract reason which is at the end of the string
    df["reason"] = df["Notes"].str.extract(r"compliant for (.+)$")

    df["entryID"] = entryID

    # Fetch the names from BigQuery
    # names_query = f"SELECT id, name FROM `student_info.roster`"
    # names_df = fetchDataFromBigQuery(names_query)

    print("Merging data...")

    # make sure the id column is of type integer
    df["id"] = df["id"].astype(int)

    # Merge the fetched names with the original DataFrame
    # df = pd.merge(df, names_df, on="id", how="left")

    # Drop the original "Notes" column
    df = df.drop(columns=["Notes"])

    return df


def doWork():
    """
    Main function for the script.
    """
    print(Style.BRIGHT + f"Starting {scriptName} script...")

    # Read in the first .csv file found
    csv_file, rawDataFrame = readCSV(source_folder)

    # Clean the data
    cleanedDataFrame = clean_data(rawDataFrame)

    # DISABLED -- delete and upload are ONE operation; enable together.

    # deleteAllDataFromTable(project_id, dataset_id, table_id)

    # Upload the data to BigQuery
    # DISABLED -- enable when you decide to pull.
    # uploadToBigQuery(cleanedDataFrame, tableSchema, project_id, dataset_id, table_id)

    # Archive the source file
    archiveSourceFile(cleanedDataFrame, csv_file, source_folder, table_id)

    print(Style.BRIGHT + f"{scriptName} script has been completed.")


if __name__ == "__main__":
    doWork()
