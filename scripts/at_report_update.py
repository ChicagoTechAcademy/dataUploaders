from _dataManager import (
    readCSV,
    uploadToBigQuery,
    deleteDataBetweenDates,
    archiveSourceFile,
    convertToStandardDate,
    getSchoolYear,
    getSemester,
)

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
    {"name": "name", "type": "STRING"},
    {"name": "id", "type": "INTEGER"},
    {"name": "date", "type": "DATE"},
    {"name": "code", "type": "STRING"},
    {"name": "course", "type": "STRING"},
    {"name": "class", "type": "STRING"},
    {"name": "period", "type": "STRING"},
    {"name": "tardy", "type": "BOOLEAN"},
    {"name": "absent", "type": "BOOLEAN"},
    {"name": "yog", "type": "INTEGER"},
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
}


def cleanData(df):
    """
    Cleans, processes and renames columns.
    """
    print(Fore.RESET + "Processing data...")

    # Rename columns
    df.rename(columns=column_mappings, inplace=True)

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
