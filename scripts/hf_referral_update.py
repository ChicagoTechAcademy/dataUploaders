from _dataManager import *


scriptName = "hf_referral_update"


# Constants
project_id = "chitechdb"
dataset_id = "behavior"
table_id = "HF-Referrals"
source_folder = f"../dataUploaders/{table_id}"

tableSchema = [
    {"name": "date", "type": "DATE"},
    {"name": "id", "type": "INTEGER"},
    {"name": "name", "type": "STRING"},
    {"name": "grade", "type": "INTEGER"},
    {"name": "race", "type": "STRING"},
    {"name": "ethnicity", "type": "STRING"},
    {"name": "gender", "type": "STRING"},
    {"name": "type", "type": "STRING"},
    {"name": "teacher", "type": "STRING"},
    {"name": "is_resolved", "type": "STRING"},
    {"name": "resolved_by", "type": "STRING"},
    {"name": "yog", "type": "INTEGER"},
]

column_mappings = {
    "date": "date",
    "student_id": "id",
    "student_name": "name",
    "grade": "grade",
    "race": "race",
    "ethnicity": "ethnicity",
    "gender": "gender",
    "name": "type",
    "teacher_that_created": "teacher",
    "teacher_that_resolved": "resolved_by",
    "referral_comment": "referral_comment",
    "is_resolved": "is_resolved",
    "is_resolved_comment": "is_resolved_comment",
    "referral_outcome_type_name": "referral_outcome_type_name",
}


def calculate_yog(grade):
    """Calculate year of graduation based on grade."""
    return {9: 2027, 10: 2026, 11: 2025, 12: 2024}.get(grade, None)


def cleanData(df):
    """Clean and preprocess the dataframe."""
    df = df.drop(df.columns[13], axis=1)
    df = df.drop(df.columns[12], axis=1)
    df = df.drop(df.columns[10], axis=1)

    # Rename columns
    for old_col, new_col in column_mappings.items():
        if old_col in df.columns and new_col not in df.columns:
            df.rename(columns={old_col: new_col}, inplace=True)

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

    print(Fore.GREEN + f"{scriptName} script has finished.")

doWork()