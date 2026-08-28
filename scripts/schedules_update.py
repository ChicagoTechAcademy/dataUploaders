import os
import pandas as pd
from _dataManager import (
    findExportFile,
    uploadToBigQuery,
    deleteAllDataFromTable,
    archiveSourceFile,
    getSchoolYear,
)

from colorama import init, Fore, Style
init(autoreset=True)


scriptName = "schedule_update"


# Constants
project_id = "chitechdb"
dataset_id = "schedules"
table_id = "studentSchedules"
source_folder = f"../dataUploaders/{table_id}"

# Order matches schedules.studentSchedules exactly. This matters more here than
# elsewhere: the load runs AFTER deleteAllDataFromTable has emptied the table,
# so a schema rejection would leave it empty.
tableSchema = [
    {"name": "id", "type": "INTEGER"},
    {"name": "classCode", "type": "STRING"},
    {"name": "class", "type": "STRING"},
    {"name": "teacher", "type": "STRING"},
    {"name": "period", "type": "STRING"},
    {"name": "entryID", "type": "INTEGER"},
    {"name": "sy", "type": "STRING"},
    {"name": "semester", "type": "STRING"},
]

ORDERED_COLS = [
    "id",
    "classCode",
    "class",
    "teacher",
    "period",
    "entryID",
    "sy",
    "semester",
]

# "Name" in this export is the TEACHER's name; the student's name is split
# across LastName / FirstName and is not loaded.
column_mappings = {
    "Student ID": "id",
    "Class": "classCode",
    "Description": "class",
    "Name": "teacher",
    "Schedule": "period",
}

# Columns present in the export that are deliberately not loaded.
DROP_COLS = [
    "LastName",
    "FirstName",
    "MiddleName",
    "SpecialEdStus",
    "Grade",
    "Homeroom",
    "Inclusion?",
    "SecType",
    "Clssrm",
    "Total",
    "Max",
]

# A ~250-student school, roughly 10 courses each.
PLAUSIBLE_ROWS = (1200, 4000)


# Get the entry ID from the user
def getEntryID():
    # entryID = input(Fore.CYAN + "Enter the entry ID for this roster YYYYMMDDX: ")

    # find todays date, and reformat it as YYYYMMDD,
    entryID = pd.Timestamp.now().strftime("%Y%m%d")

    print(Fore.CYAN + f"The EntryID is {entryID}.")

    return int(entryID)


def getSemester(classCode):
    """
    Derives the semester from the course code.

    The code is positional: character 6 is the course type ("N" marks a non-class
    like the meal period) and character 5 is the semester digit. For example
    "184401R-002-C" -> character 5 is "1" -> S1.

    Returns None rather than raising when the code is too short or missing, so a
    single malformed row cannot take down the run.
    """
    if not isinstance(classCode, str) or len(classCode) < 7:
        return None
    if classCode[6] == "N":
        return "Other"
    if classCode[5] == "1":
        return "S1"
    if classCode[5] == "2":
        return "S2"
    return None


def readExport(source_folder):
    """
    Reads the schedules export.

    An ordinary CSV with a single header row -- no pagination and no layout
    drift, unlike the attendance and grades reports. The file carries a UTF-8
    BOM, which pandas strips on its own.
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
    missing = [c for c in column_mappings if c not in df.columns]
    if missing:
        raise SystemExit(
            Fore.YELLOW
            + f"Export is missing expected column(s): {', '.join(missing)}.\n"
            + f"Columns found: {', '.join(map(str, df.columns))}\n"
            + "ASPEN may have renamed a column, or this is the wrong report."
        )


def checkSchedules(df):
    """
    Reports anything odd. Advisory only -- this export carries no totals row to
    reconcile against.
    """
    print(Fore.GREEN + "Checking the schedules pull...")
    print(Fore.GREEN + f"  {len(df)} enrollments across {df['id'].nunique()} students "
                       f"and {df['classCode'].nunique()} sections.")

    low, high = PLAUSIBLE_ROWS
    if not (low <= len(df) <= high):
        print(Fore.YELLOW + f"  Row count {len(df)} is outside the usual range {low}-{high}.")

    noSemester = int(df["semester"].isna().sum())
    if noSemester:
        examples = df.loc[df["semester"].isna(), "classCode"].unique()[:5].tolist()
        print(Fore.YELLOW + f"  {noSemester} row(s) have no semester derived from classCode. "
                            f"Examples: {examples}")

    print(Fore.GREEN + f"  semester split: {df['semester'].value_counts(dropna=False).to_dict()}")


def cleanData(df):
    """
    Processes the dataframe for cleanup tasks.
    """
    print("Cleaning data...")

    validateExport(df)

    # Drop unnecessary columns
    df = df.drop(columns=DROP_COLS, errors="ignore")

    # Rename columns based on predefined mappings
    df = df.rename(columns=column_mappings, errors="raise")

    # Rows with a non-numeric Student ID are footer junk, not students.
    id_num = pd.to_numeric(df["id"], errors="coerce")
    isStudent = id_num.notna()
    dropped = int((~isStudent).sum())
    if dropped:
        print(Fore.YELLOW + f"Dropping {dropped} row(s) whose Student ID is not numeric.")
    df = df[isStudent].copy()
    df["id"] = id_num[isStudent].astype("int64")

    # Add 'entryID' column
    df["entryID"] = getEntryID()

    # Add 'sy' & 'semester' columns
    # Derive the school year rather than hardcoding it; see _dataManager.getSchoolYear
    df["sy"] = getSchoolYear(pd.Timestamp.now().strftime("%Y-%m-%d"))
    df["semester"] = df["classCode"].apply(getSemester)

    df = df[ORDERED_COLS]

    return df


def doWork():
    """
    Main function for the script.
    """
    print(Style.BRIGHT + f"Starting {scriptName} script...")

    # Read in the export. The source file is never opened for writing.
    csv_file, rawDataFrame = readExport(source_folder)

    # Clean the data
    cleanedDataFrame = cleanData(rawDataFrame)

    # Report anything odd. Advisory only -- no totals row in this export.
    checkSchedules(cleanedDataFrame)

    # This table is current-state-only: the whole thing is replaced each pull.
    # ENABLED. THE DELETE AND THE UPLOAD ARE ONE OPERATION -- never comment out
    # one without the other. Note the delete lands FIRST, so if the upload fails
    # the table is left empty; the export survives (archiving runs after), so the
    # fix is to re-run rather than re-pull from ASPEN.
    deleteAllDataFromTable(project_id, dataset_id, table_id)
    uploadToBigQuery(cleanedDataFrame, tableSchema, project_id, dataset_id, table_id)

    # Archive the source file
    # NOTE: archiveSourceFile writes the cleaned frame to archivedFiles/ and then
    # DELETES the source export. Keep a spare copy while testing.
    archiveSourceFile(cleanedDataFrame, csv_file, source_folder, table_id)

    print(Style.BRIGHT + f"{scriptName} data pull complete.")


if __name__ == "__main__":
    doWork()
