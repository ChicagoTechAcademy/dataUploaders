from _dataManager import *
from datetime import date


scriptName = "grades_update"


# Constants
project_id = "chitechdb"
dataset_id = "academics"
table_id = "grades"
source_folder = f"../dataUploaders/{table_id}"

tableSchema = [
    {"name": "checkpoint", "type": "STRING"},
    {"name": "date", "type": "DATE"},
    {"name": "id", "type": "INTEGER"},
    {"name": "name", "type": "STRING"},
    {"name": "yog", "type": "INTEGER"},
    {"name": "course", "type": "STRING"},
    {"name": "teacher", "type": "STRING"},
    {"name": "gradePercent", "type": "FLOAT"},
    {"name": "letterGrade", "type": "STRING"},
    {"name": "sy", "type": "STRING"},
]

COLUMN_NAMES = [
    "yog",
    "name",
    "drop1",
    "drop2",
    "id",
    "course",
    "drop3",
    "teacher",
    "drop4",
    "averageAndLetter",
]



def cleanData(df):
    """
    Cleans and processes data.
    """
    print(Fore.RESET + "Cleaning data...")
    df = df.copy()
    df.columns = COLUMN_NAMES
    checkpoint = input("Enter the Checkpoint in 'Q# CP#' format: ")

    # Processing
    df["id"] = pd.to_numeric(df["id"], errors="coerce")
    df.dropna(subset=["id"], inplace=True)
    df[["gradePercent", "letterGrade"]] = df["averageAndLetter"].str.split(expand=True)
    df.drop(columns=["averageAndLetter"], inplace=True)
    today = date.today()
    df["checkpoint"] = f"SY24: {checkpoint}"
    df["date"] = today.strftime("%Y-%m-%d")
    df["sy"] = "SY24"
    df["id"] = df["id"].astype(int)
    df["yog"] = df["yog"].astype(int)
    df["gradePercent"] = df["gradePercent"].astype(float)
    df = df[
        [
            "checkpoint",
            "date",
            "id",
            "name",
            "yog",
            "course",
            "teacher",
            "gradePercent",
            "letterGrade",
            "sy",
        ]
    ]

    print(Fore.RESET + "Merging data...")
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
