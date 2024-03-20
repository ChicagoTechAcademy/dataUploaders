from _dataManager import *


scriptName = "hf_events_update"


# Constants
project_id = "chitechdb"
dataset_id = "behavior"
table_id = "HF-Events"
source_folder = f"../dataUploaders/{table_id}"

tableSchema = [
    {"name": "date","type": "DATE"},
    {"name": "id","type": "INTEGER"},
    {"name": "name","type": "STRING"},
    {"name": "grade","type": "INTEGER"},
    {"name": "race","type": "STRING"},
    {"name": "ethnicity","type": "STRING"},
    {"name": "gender","type": "STRING"},
    {"name": "event_group","type": "STRING"},
    {"name": "event_type","type": "STRING"},
    {"name": "class","type": "STRING"},
    {"name": "teacher","type": "STRING"},
    {"name": "is_resolved","type": "STRING"},
    {"name": "yog","type": "INTEGER"},
]

# Mapping of original column names to new names
column_mappings = {
    "date": "date",
    "student_id": "id",
    "student_name": "name",
    "grade": "grade",
    "race": "race",
    "ethnicity": "ethnicity",
    "gender": "gender",
    "event_group": "event_group",
    "event_type": "event_type",
    "class": "class",
    "teacher": "teacher",
    "is_resolved": "is_resolved",
}


def calculate_yog(grade):
    """Calculate year of graduation based on grade."""
    return {9: 2027, 10: 2026, 11: 2025, 12: 2024}.get(grade, None)


def cleanData(df):
    """Clean and process data."""
    df.drop(columns=df.columns[12:14], inplace=True)
    df.rename(columns=column_mappings, inplace=True)
    df["yog"] = df["grade"].apply(calculate_yog)
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
