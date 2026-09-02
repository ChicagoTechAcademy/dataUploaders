import pandas as pd
from _dataManager import (
    readPaginatedExport,
    reconcile,
    uploadToBigQuery,
    archiveSourceFile,
)

from colorama import init, Fore, Style
init(autoreset=True)


scriptName = "Total Avg Daily Attendance Update"


# Constants
project_id = "chitechdb"
dataset_id = "attendance"
table_id = "totalAttendance"
source_folder = f"../dataUploaders/{table_id}"

# NOTE: gradeLevel is a NEW column, produced for free by the page parser below
# (it comes from each page's banner, not from a data column). Add it to the
# BigQuery table before re-enabling the upload in doWork().
tableSchema = [
    {"name": "entryID", "type": "INTEGER"},
    {"name": "week", "type": "STRING"},
    {"name": "id", "type": "INTEGER"},
    {"name": "name", "type": "STRING"},
    {"name": "gradeLevel", "type": "STRING"},
    {"name": "daysEnrolled", "type": "FLOAT"},
    {"name": "daysNotEnrolled", "type": "FLOAT"},
    {"name": "daysPresent", "type": "FLOAT"},
    {"name": "daysExcused", "type": "FLOAT"},
    {"name": "daysNotExcused", "type": "FLOAT"},
]

ORDERED_COLS = [
    "entryID",
    "week",
    "id",
    "name",
    "gradeLevel",
    "daysEnrolled",
    "daysNotEnrolled",
    "daysPresent",
    "daysExcused",
    "daysNotExcused",
]

# The export's own header text -> our column name. Header text is rebuilt by
# joining each page's two header rows, so "Days" over "Enrolled" becomes
# "Days Enrolled". Match on these names, never on a fixed column index.
FIELD_MAP = {
    "ID": "id",
    "Name": "name",
    "Days Enrolled": "daysEnrolled",
    "Days Not Enrolled": "daysNotEnrolled",
    "Days Present": "daysPresent",
    "Days Excused": "daysExcused",
    "Days Not Excused": "daysNotExcused",
}

# Labels in the report's own GRAND TOTALS block -> our column name. The totals
# block spells two of them differently from the column headers.
TOTALS_MAP = {
    "Days Enrolled": "daysEnrolled",
    "Days Not Enrolled": "daysNotEnrolled",
    "Days Present": "daysPresent",
    "Days Excused Absent": "daysExcused",
    "Days Unexcused Absent": "daysNotExcused",
}

# Columns checked against the report's own totals
RECONCILE_COLS = [
    "daysEnrolled",
    "daysNotEnrolled",
    "daysPresent",
    "daysExcused",
    "daysNotExcused",
]


# Get the entry ID from the user
def getEntryID():
    # entryID = input(Fore.CYAN + "Enter the entry ID for this roster YYYYMMDDX: ")

    # find todays date, and reformat it as YYYYMMDD,
    entryID = pd.Timestamp.now().strftime("%Y%m%d")

    print(Fore.CYAN + f"The EntryID is {entryID}.")

    return int(entryID)

def getWeek():
    return input(Fore.CYAN + "Enter the week/label for this data (W02, EOW02, ...): ")


# Clean the data
def cleanData(df):
    entryID = getEntryID()
    week = getWeek()

    # Keep only 8-digit student IDs. The parser already drops report furniture,
    # so anything caught here is unexpected and worth saying out loud.
    id_str = df["id"].astype(str).str.strip()
    mask_8_digit = id_str.str.fullmatch(r"\d{8}").fillna(False)
    dropped = int((~mask_8_digit).sum())
    if dropped:
        print(Fore.YELLOW + f"Dropping {dropped} row(s) whose id is not an 8-digit number.")
    df = df[mask_8_digit].copy()

    df["id"] = df["id"].astype(str).str.strip().astype(int)
    df["entryID"] = int(entryID)
    df["week"] = week

    numeric_cols = [
        "daysEnrolled",
        "daysNotEnrolled",
        "daysPresent",
        "daysExcused",
        "daysNotExcused",
    ]
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype(float)

    # Reorder columns to match the BigQuery schema
    df = df[ORDERED_COLS]

    return df

def doWork():
    """
    Main function for the script.
    """
    print(Style.BRIGHT + f"Starting {scriptName} script...")

    # Read and repair the export in memory. Nothing is written back to it.
    csv_file, rawDataFrame, totals = readPaginatedExport(
        source_folder, FIELD_MAP, TOTALS_MAP
    )

    # Clean the data
    cleanedDataFrame = cleanData(rawDataFrame)

    # Verify the parse against the report's own totals before writing anything.
    # A failure aborts before both the upload and the archive -- archiveSourceFile
    # deletes the source export, so a bad parse must not reach it.
    if not reconcile(cleanedDataFrame, totals, RECONCILE_COLS):
        print(Fore.YELLOW + "Aborting: nothing uploaded, source file left in place.")
        raise SystemExit(1)

    # Upload the data to BigQuery
    # DISABLED for testing.
    uploadToBigQuery(cleanedDataFrame, tableSchema, project_id, dataset_id, table_id)

    # Archive the source file
    # NOTE: archiveSourceFile writes the cleaned frame to archivedFiles/ and then
    # DELETES the source export. Keep a spare copy while testing.
    archiveSourceFile(cleanedDataFrame, csv_file, source_folder, table_id)

    print(Style.BRIGHT + f"{scriptName} data pull complete.")


if __name__ == "__main__":
    doWork()
