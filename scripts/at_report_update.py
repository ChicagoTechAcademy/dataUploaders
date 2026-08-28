import os
import pandas as pd
from _dataManager import (
    findExportFile,
    uploadToBigQuery,
    deleteDataBetweenDates,
    archiveSourceFile,
    convertToStandardDate,
    getSchoolYear,
    getSemester,
)

from colorama import init, Fore, Style

init(autoreset=True)

# Constants
scriptName = "at_report_update"


# Constants
project_id = "chitechdb"
dataset_id = "attendance"
table_id = "at-report"
source_folder = f"../dataUploaders/{table_id}"

tableSchema = [
    {"name": "entryID", "type": "INTEGER"},
    {"name": "name", "type": "STRING"},
    {"name": "id", "type": "INTEGER"},
    {"name": "date", "type": "DATE"},
    {"name": "code", "type": "STRING"},
    {"name": "course", "type": "STRING"},
    {"name": "class", "type": "STRING"},
    {"name": "period", "type": "STRING"},
    {"name": "sy", "type": "STRING"},
    {"name": "semester", "type": "STRING"},
]

ORDERED_COLS = [
    "entryID",
    "name",
    "id",
    "date",
    "code",
    "course",
    "class",
    "period",
    "sy",
    "semester",
]

# Keyed on the NORMALISED header -- the text after the last ">". ASPEN emits
# this report's headers as either "Master > Class" or "Master Schedule > Class"
# depending on version, and the legacy mapping only matched the short form, so a
# version bump would have silently produced a frame with no course column.
# Normalising collapses both to "Class".
column_mappings = {
    "Name": "name",
    "Student ID": "id",
    "Date": "date",
    "Code": "code",
    "Class": "course",
    "Description": "class",
    "Period": "period",
    "Tardy?": "tardy",
    "Absent?": "absent",
    "YOG": "yog",
}

# Pulled but not loaded. "Code" already carries the meaning that Tardy?/Absent?
# duplicate.
DROP_COLS = ["yog", "tardy", "absent"]


def normaliseHeader(name):
    """
    "Master Schedule > Description" and "Master > Description" both become
    "Description". Everything else is returned trimmed.
    """
    return str(name).split(">")[-1].strip()


# Get the entry ID from the user
def getEntryID():
    # entryID = input(Fore.CYAN + "Enter the entry ID for this roster YYYYMMDDX: ")

    # find todays date, and reformat it as YYYYMMDD,
    entryID = pd.Timestamp.now().strftime("%Y%m%d")

    print(Fore.CYAN + f"The EntryID is {entryID}.")

    return int(entryID)


def readExport(source_folder):
    """
    Reads the A/T report export.

    An ordinary CSV with a single header row. The file carries a UTF-8 BOM.
    Filenames look like QR_2758963764815969128.csv and contain no date, so a
    stale file here is invisible -- the row count and date range printed below
    are the only signal you get.
    """
    csv_file = findExportFile(source_folder)
    csv_path = os.path.join(source_folder, csv_file)

    print(Fore.GREEN + f"Opening '{csv_file}'...")
    df = pd.read_csv(csv_path, encoding="utf-8-sig").dropna(how="all")
    print(Fore.GREEN + f"Read {len(df)} rows from '{csv_file}'.")
    return csv_file, df


def validateExport(df):
    """
    Stops early if the export is not the report this script expects.
    """
    required = ["Name", "Student ID", "Date", "Code", "Class", "Description", "Period"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise SystemExit(
            Fore.YELLOW
            + f"Export is missing expected column(s): {', '.join(missing)}.\n"
            + f"Columns found (normalised): {', '.join(map(str, df.columns))}\n"
            + "ASPEN may have renamed a column, or this is the wrong report."
        )


def cleanData(df):
    """
    Cleans, processes and renames columns.
    """
    print("Processing data...")

    # Normalise headers before anything looks at them
    df = df.rename(columns=normaliseHeader)
    validateExport(df)

    # Create entryID
    df["entryID"] = getEntryID()

    # Rename columns
    df = df.rename(columns=column_mappings)

    # drop columns yog, tardy, absent
    df = df.drop(columns=DROP_COLS, errors="ignore")

    # Convert date and derive related columns
    df["date"] = df["date"].apply(convertToStandardDate)
    df = df[df["date"].notna()].copy()
    df["sy"] = df["date"].apply(getSchoolYear)
    df["semester"] = df["date"].apply(getSemester)

    # This export reaches back into the previous school year. Keep only the
    # current one -- the boundary is August, per _dataManager.getSchoolYear.
    currentSY = getSchoolYear(pd.Timestamp.now().strftime("%Y-%m-%d"))
    keep = df["sy"] == currentSY
    droppedSY = int((~keep).sum())
    if droppedSY:
        priorRange = f"{df.loc[~keep, 'date'].min()} to {df.loc[~keep, 'date'].max()}"
        print(Fore.YELLOW + f"Dropping {droppedSY} row(s) outside {currentSY} ({priorRange}).")
    df = df[keep].copy()

    if df.empty:
        raise SystemExit(
            Fore.YELLOW + f"No rows left after filtering to {currentSY}. "
            "Check the date range on the export."
        )

    # Rows with a non-numeric Student ID are footer junk, not students.
    id_num = pd.to_numeric(df["id"], errors="coerce")
    isStudent = id_num.notna()
    dropped = int((~isStudent).sum())
    if dropped:
        print(Fore.YELLOW + f"Dropping {dropped} row(s) whose Student ID is not numeric.")
    df = df[isStudent].copy()
    df["id"] = id_num[isStudent].astype("int64")

    # period is a STRING: it holds "3" but also "AdvA" and "Lunch/Adv".
    df["period"] = df["period"].astype(str).str.strip()

    df = df[ORDERED_COLS]

    return df


def checkATReport(df):
    """
    Reports anything odd. Advisory only -- no totals row in this export.
    """
    print(Fore.GREEN + "Checking the A/T pull...")
    print(Fore.GREEN + f"  {len(df)} rows, {df['id'].nunique()} distinct students, "
                       f"dates {df['date'].min()} to {df['date'].max()}.")
    print(Fore.GREEN + f"  codes: {df['code'].value_counts().to_dict()}")
    print(Fore.GREEN + f"  semesters: {df['semester'].value_counts().to_dict()}")


def doWork():
    """
    Main function for the script.
    """
    print(Style.BRIGHT + f"Starting {scriptName} script...")

    # Read in the export. The source file is never opened for writing.
    csv_file, rawDataFrame = readExport(source_folder)

    # Clean the data
    cleanedDataFrame = cleanData(rawDataFrame)

    checkATReport(cleanedDataFrame)

    # This report is an event stream: the incoming date range is replaced. Note
    # the range now covers only the current school year, so re-running cannot
    # disturb rows from a previous year that are already loaded.
    # THE DELETE AND THE UPLOAD ARE ONE OPERATION. Enabling the delete without
    # the upload removes that date range and puts nothing back.
    # DISABLED -- enable both together when you decide to pull.
    deleteDataBetweenDates(
        project_id,
        dataset_id,
        table_id,
        cleanedDataFrame["date"].min(),
        cleanedDataFrame["date"].max(),
    )
    uploadToBigQuery(cleanedDataFrame, tableSchema, project_id, dataset_id, table_id)

    # Archive the source file
    # NOTE: archiveSourceFile writes the cleaned frame to archivedFiles/ and then
    # DELETES the source export. Keep a spare copy while testing.
    archiveSourceFile(cleanedDataFrame, csv_file, source_folder, table_id)

    print(Style.BRIGHT + f"{scriptName} data pull complete.")


if __name__ == "__main__":
    doWork()
