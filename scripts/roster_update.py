import pandas as pd
import os
from _dataManager import (
    findExportFile,
    uploadToBigQuery,
    archiveSourceFile,
    getSchoolYear,
)

from colorama import init, Fore, Style
init(autoreset=True)


scriptName = "roster_update"


# Constants
project_id = "chitechdb"
dataset_id = "studentInfo"
table_id = "roster"
source_folder = f"../dataUploaders/{table_id}"

tableSchema = [
    {"name": "entryID", "type": "INTEGER"},
    {"name": "id", "type": "INTEGER"},
    {"name": "name", "type": "STRING"},
    {"name": "yog", "type": "INTEGER"},
    {"name": "homeroom", "type": "STRING"},
    {"name": "sy", "type": "STRING"},
]

ORDERED_COLS = ["entryID", "id", "name", "yog", "homeroom", "sy"]

# The export carries 22 columns; these are the four that are used.
column_mappings = {
    "Name": "name",
    "Student ID": "id",
    "YOG": "yog",
    "Homeroom": "homeroom",
}

# Enrollment was 243 as of Aug 2026 (confirmed against studentInfo.roster and
# the schedules export). Kept wide on purpose -- this only flags an export that
# is obviously a partial selection or the wrong report.
PLAUSIBLE_ROWS = (150, 700)


# Get the entry ID from the user
def getEntryID():
    # entryID = input(Fore.CYAN + "Enter the entry ID for this roster YYYYMMDDX: ")

    # find todays date, and reformat it as YYYYMMDD,
    entryID = pd.Timestamp.now().strftime("%Y%m%d")

    # The trailing "1" this used to append has been dropped, so entryID is now
    # a plain YYYYMMDD like every other script.
    #
    # HISTORICAL ROWS STILL CARRY THE OLD 9-DIGIT FORM. Because entryID is an
    # INTEGER, 202608271 is numerically larger than 20260828, so MAX(entryID)
    # and ORDER BY entryID DESC will keep returning old pulls until the existing
    # rows are backfilled. See NOTES-TODO.md.

    print(Fore.CYAN + f"The EntryID is {entryID}.")

    return int(entryID)


def readExport(source_folder):
    """
    Reads the roster export.

    Unlike the attendance reports this is an ordinary CSV with a header row, so
    it needs no page parsing -- only a guarantee about which file was read.
    """
    csv_file = findExportFile(source_folder)
    csv_path = os.path.join(source_folder, csv_file)

    print(Fore.GREEN + f"Opening '{csv_file}'...")
    df = pd.read_csv(csv_path).dropna(how="all")
    print(Fore.GREEN + f"Read {len(df)} rows from '{csv_file}'.")
    return csv_file, df


def validateExport(df):
    """
    Checks the export before any of it is trusted.

    The roster has no totals block to reconcile against, so these stand in for
    it: the columns are the ones expected, and the shape is plausible.
    """
    missing = [c for c in column_mappings if c not in df.columns]
    if missing:
        raise SystemExit(
            Fore.YELLOW
            + f"Export is missing expected column(s): {', '.join(missing)}.\n"
            + f"Columns found: {', '.join(map(str, df.columns))}\n"
            + "ASPEN may have renamed a column, or this is the wrong report."
        )


def checkRoster(df):
    """
    Reports anything odd about the cleaned frame.

    These are advisory only -- unlike the attendance reports, the roster export
    carries no totals block to reconcile against, so there is nothing here
    certain enough to justify blocking a load that would otherwise be fine.
    Flip the caller in doWork() to abort if that changes.
    """
    print(Fore.GREEN + "Checking the roster...")
    ok = True

    print(Fore.GREEN + f"  {len(df)} students. Compare against the count ASPEN showed "
                       f"(\"0 of N selected\").")

    low, high = PLAUSIBLE_ROWS
    if not (low <= len(df) <= high):
        print(Fore.YELLOW + f"  Row count {len(df)} is outside the usual range {low}-{high}.")
        ok = False

    duplicates = df["id"].duplicated().sum()
    if duplicates:
        dupIDs = sorted(df.loc[df["id"].duplicated(keep=False), "id"].unique())
        print(Fore.YELLOW + f"  {duplicates} duplicate student ID(s): {dupIDs[:10]}")
        print(Fore.YELLOW + "  A roster pull should hold one row per student.")
        ok = False

    blankNames = int(df["name"].isna().sum() + (df["name"].astype(str).str.strip() == "").sum())
    if blankNames:
        print(Fore.YELLOW + f"  {blankNames} row(s) have a blank name.")
        ok = False

    if ok:
        print(Fore.GREEN + "  Roster looks sane.")
    else:
        print(Fore.YELLOW + "  Continuing anyway -- these are warnings, not errors.")
    return ok


def cleanData(df):
    entryID = getEntryID()

    validateExport(df)

    # The data has 22 columns. Only four are needed. Drop the rest.
    df = df[list(column_mappings.keys())].copy()
    df = df.rename(columns=column_mappings)

    # Rows with a non-numeric Student ID are footer junk, not students.
    # Coerce rather than regex-matching the string form: a single blank cell in
    # the export turns the whole column to float64, and every id then renders as
    # "90000000.0", which no digit pattern will match.
    id_num = pd.to_numeric(df["id"], errors="coerce")
    isStudent = id_num.notna()
    dropped = int((~isStudent).sum())
    if dropped:
        print(Fore.YELLOW + f"Dropping {dropped} row(s) whose Student ID is not numeric.")
    df = df[isStudent].copy()

    df["id"] = id_num[isStudent].astype("int64")
    df["yog"] = pd.to_numeric(df["yog"], errors="coerce").astype("Int64")
    # Homerooms are alphanumeric ("A606"), so this column normally stays object
    # dtype. Guard the case where a blank cell turns it float and every value
    # would otherwise render as "606.0".
    homeroom = df["homeroom"]
    if pd.api.types.is_float_dtype(homeroom):
        homeroom = homeroom.astype("Int64")
    df["homeroom"] = homeroom.astype(str).str.strip().replace({"<NA>": "", "nan": ""})

    df["entryID"] = int(entryID)
    # Derive the school year rather than hardcoding it; see _dataManager.getSchoolYear
    df["sy"] = getSchoolYear(pd.Timestamp.now().strftime("%Y-%m-%d"))

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

    # Report anything odd, but do not block: this script has been running fine,
    # and these checks are heuristics rather than a checksum. To make them
    # blocking, wrap this in "if not ...: raise SystemExit(1)".
    checkRoster(cleanedDataFrame)

    # Upload the data to BigQuery
    # DISABLED for testing.
    uploadToBigQuery(cleanedDataFrame, tableSchema, project_id, dataset_id, table_id)

    # Archive the source file
    archiveSourceFile(cleanedDataFrame, csv_file, source_folder, table_id)

    print(Style.BRIGHT + f"{scriptName} data pull complete.")


if __name__ == "__main__":
    doWork()
