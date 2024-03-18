from _dataManager import (
    readCSV,
    uploadToBigQuery,
    deleteAllDataFromTable,
    archiveSourceFile,
    convertToStandardDate,
)

from colorama import init, Fore
init(autoreset=True)


scriptName = "roster_update"


# Constants
project_id = "chitechdb"
dataset_id = "student_info"
table_id = "roster"
source_folder = f"../dataUploaders/{table_id}"

tableSchema = [
    {"name": "name", "type": "STRING"},
    {"name": "id", "type": "INTEGER"},
    {"name": "dob", "type": "DATE"},
    {"name": "enrollment", "type": "STRING"},
    {"name": "yog", "type": "INTEGER"},
]


# Define the column name mappings
column_mappings = {
    "Name": "name",
    "Student ID": "id",
    "DOB": "dob",
    "EnrStatus": "enrollment",
    "YOG": "yog",
}


def cleanData(df):
    df = df.drop(df.columns[18], axis=1)
    df = df.drop(df.columns[17], axis=1)
    df = df.drop(df.columns[16], axis=1)
    df = df.drop(df.columns[15], axis=1)
    df = df.drop(df.columns[14], axis=1)
    df = df.drop(df.columns[13], axis=1)
    df = df.drop(df.columns[10], axis=1)
    df = df.drop(df.columns[9], axis=1)
    df = df.drop(df.columns[8], axis=1)
    df = df.drop(df.columns[7], axis=1)
    df = df.drop(df.columns[6], axis=1)
    df = df.drop(df.columns[4], axis=1)
    df = df.drop(df.columns[3], axis=1)
    df = df.drop(df.columns[2], axis=1)

    # Rename the columns that exist in the DataFrame and are specified in column_mappings
    for old_col, new_col in column_mappings.items():
        if old_col in df.columns:
            df.rename(columns={old_col: new_col}, inplace=True)

    df["name"] = df["name"].astype(str)
    df["id"] = df["id"].astype(int)
    df["yog"] = df["yog"].astype(int)
    df["enrollment"] = df["enrollment"].astype(str)
    df["dob"] = df["dob"].apply(convertToStandardDate)
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
    deleteAllDataFromTable(project_id, dataset_id, table_id)

    # Upload the data to BigQuery
    uploadToBigQuery(cleanedDataFrame, tableSchema, project_id, dataset_id, table_id)

    # Archive the source file
    archiveSourceFile(cleanedDataFrame, csv_file, source_folder, table_id)

    print(Fore.GREEN + f"{scriptName} data pull complete.")


doWork()
