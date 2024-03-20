from _dataManager import *


scriptName = "medicalCompliance"


# Constants
project_id = "chitechdb"
dataset_id = "student_info"
table_id = "medicalCompliance"
source_folder = f"../dataUploaders/{table_id}"

tableSchema = [
    {"name": "id", "type": "STRING"},
    {"name": "name", "type": "STRING"},
    {"name": "reason", "type": "STRING"},
]


def clean_data(df):
    """
    Cleans and processes data.
    """
    print(Fore.RESET + "Cleaning data...")

    # Drop the "School" column
    df = df.drop(columns=["School"])

    # Extract student ID and reason from the "Notes" column
    df["id"] = df["Notes"].str.extract(r"Student # (\d+)").astype("Int64")
    # Extract reason which is at the end of the string
    df["reason"] = df["Notes"].str.extract(r"compliant for (.+)$")

    # Fetch the names from BigQuery
    names_query = f"SELECT id, name FROM `student_info.roster`"
    names_df = fetchDataFromBigQuery(names_query)

    print(Fore.RESET + "Merging data...")

    # Merge the fetched names with the original DataFrame
    df = pd.merge(df, names_df, on="id", how="left")

    # Drop the original "Notes" column
    df = df.drop(columns=["Notes"])

    return df


def doWork():
    """
    Main function for the script.
    """
    print(Fore.YELLOW + f"Starting {scriptName} script...")

    # Read in the first .csv file found
    csv_file, rawDataFrame = readCSV(source_folder)

    # Clean the data
    cleanedDataFrame = clean_data(rawDataFrame)

    deleteAllDataFromTable(project_id, dataset_id, table_id)

    # Upload the data to BigQuery
    uploadToBigQuery(cleanedDataFrame, tableSchema, project_id, dataset_id, table_id)

    # Archive the source file
    archiveSourceFile(cleanedDataFrame, csv_file, source_folder, table_id)

    print(Fore.GREEN + f"{scriptName} script has been completed.")


doWork()
