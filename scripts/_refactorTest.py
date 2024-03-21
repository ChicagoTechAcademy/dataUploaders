from _dataManager import *


scriptName = "refactorTest"


# Constants
project_id = "chitechdb"
dataset_id = "test"
table_id = "testData"
source_folder = f"../dataUploaders/{table_id}"

tableSchema = [
    {"name": "name", "type": "STRING"},
    {"name": "id", "type": "INTEGER"},
    {"name": "yog", "type": "INTEGER"},
    {"name": "balance", "type": "FLOAT"},
]


def cleanData(df):
    """
    Cleans the data read in from the CSV file and returns a DataFrame.
    """
    print(Fore.CYAN + "Cleaning data...")

    # if the balance column has ( ) around it, then convert to a negative number
    df["balance"] = df["balance"].apply(lambda x: x.replace("(", "-").replace(")", "") if "(" in x else x)

    # convert the balance column to float
    df["balance"] = df["balance"].astype(float)

    print(Fore.CYAN + "Data has been cleaned.")
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

    # Fetch the roster data
    # rosterQuery = f"SELECT * FROM `student_info.roster`"
    # roster_df = fetchDataFromBigQuery(rosterQuery)

    # deleteAllDataFromTable(project_id, dataset_id, table_id)

    # Upload the data to BigQuery
    uploadToBigQuery(cleanedDataFrame, tableSchema, project_id, dataset_id, table_id)

    # Archive the source file
    archiveSourceFile(cleanedDataFrame, csv_file, source_folder, table_id)


    print(Fore.GREEN + f"{scriptName} data pull complete.")


doWork()
