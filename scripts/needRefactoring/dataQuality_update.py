from _dataManager import *


scriptName = "dataQuality_update"


# Constants
project_id = "chitechdb"
dataset_id = "logistics"
table_id = "dataQuality"
source_folder = f"../dataUploaders/{table_id}"

tableSchema = [
    {"name": "qualityArea", "type": "STRING"},
    {"name": "measure", "type": "STRING"},
    {"name": "weight", "type": "FLOAT"},
    {"name": "errorCount", "type": "INTEGER"},
    {"name": "percentError", "type": "FLOAT"},
    {"name": "grade", "type": "STRING"},
    {"name": "week", "type": "STRING"},
]

# Define the column name mappings
column_mappings = {
    "Area": "qualityArea",
    "Measure": "measure",
    "Weight": "weight",
    "Errors": "errorCount",
    "% Error": "percentError",
    "Grade": "grade",
}


def cleanData(df):
    """
    Cleans the data read in from the CSV file and returns a DataFrame.
    """
    print(Fore.WHITE + "Getting Data Quality Data from user...")
    # Prompt the user for the week number
    week_number = input("Enter the week number (e.g., W01): ")

    # Prompt the user for the number of errors, total percent errors, and grade
    num_errors = int(input("Enter the number of errors: "))
    total_percent_errors = float(input("Enter the total percent errors: "))
    grade = input("Enter the grade: ")

    print(Fore.CYAN + "Cleaning data...")

    df["week"] = week_number

    df = df.dropna(how="all")

    # Rename the columns that need renaming
    for old_col, new_col in column_mappings.items():
        if old_col in df.columns and new_col not in df.columns:
            df.rename(columns={old_col: new_col}, inplace=True)

    df["percentError"] = (
        df["percentError"].str.rstrip("%").astype(float)
    )  # Clean percentError column

    df["weight"] = (
        df["weight"].str.rstrip("%").astype(int)
    )  # Clean and convert Weight column

    # Add custom rows with user-specified values
    custom_rows = {
        "qualityArea": "total",
        "measure": ["totalErrors", "totalPercentErrors", "dataQualityGrade"],
        "weight": [None, None, None],
        "errorCount": [num_errors, num_errors, None],
        "percentError": [total_percent_errors, total_percent_errors, None],
        "grade": [grade, grade, grade],
        "week": [week_number, week_number, week_number],
    }

    custom_df = pd.DataFrame(custom_rows)
    df = pd.concat([df, custom_df], ignore_index=True)

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
