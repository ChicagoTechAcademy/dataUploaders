import os
import pandas as pd
from _dataManager import (
    findExportFile,
    uploadToBigQuery,
    deleteDataBetweenDates,
    archiveSourceFile,
    convertToStandardDate,
    getSchoolYear,
)

from colorama import init, Fore, Style
init(autoreset=True)


scriptName = "half_day_report_update"


# Constants
project_id = "chitechdb"
dataset_id = "attendance"
table_id = "half-day-report"
source_folder = f"../dataUploaders/{table_id}"

tableSchema = [
    {"name": "entryID", "type": "INTEGER"},
    {"name": "week", "type": "STRING"},
    {"name": "date", "type": "DATE"},
    {"name": "name", "type": "STRING"},
    {"name": "id", "type": "INTEGER"},
    {"name": "yog", "type": "INTEGER"},
    {"name": "code", "type": "STRING"},
    {"name": "excused", "type": "BOOLEAN"},
    {"name": "percentAbs", "type": "FLOAT"},
]

ORDERED_COLS = [
    "entryID",
    "week",
    "date",
    "name",
    "id",
    "yog",
    "code",
    "excused",
    "percentAbs",
]

COLUMN_MAPPINGS = {
    "Date": "date",
    "Student > Name": "name",
    "Student ID": "id",
    "Student > Grade": "grade",
    "Code": "code",
    "Time": "time",
    "Absent?": "absent",
    "Tardy?": "tardy",
    "Excused?": "excused",
    "PcntAbs": "percentAbs",
    "Other": "other",
}

# Pulled but not loaded.
DROP_COLS = ["time", "absent", "tardy", "other", "grade"]


# Get the entry ID from the user
def getEntryID():
    # entryID = input(Fore.CYAN + "Enter the entry ID for this roster YYYYMMDDX: ")

    # find todays date, and reformat it as YYYYMMDD,
    entryID = pd.Timestamp.now().strftime("%Y%m%d")

    print(Fore.CYAN + f"The EntryID is {entryID}.")

    return int(entryID)


def getWeek():
    return input(Fore.CYAN + "Enter the week/label for this data (W01, EndOfQ1, ...): ")


def gradeToYOG(grade, schoolYear):
    """
    Derives year of graduation from grade level and the school year.

    A grade 12 student in SY27 graduates in 2027, so YOG = year + (12 - grade).

    This replaces a hardcoded {9:2029, 10:2028, 11:2027, 12:2026} map, which was
    correct for SY26 and silently wrote every YOG a year early once the school
    year rolled over. It also only covered grades 9-12, so a grade 6 or 7 row --
    both of which appear in real exports -- produced NaN and then crashed the
    run on .astype(int).
    """
    if pd.isna(grade):
        return pd.NA
    endYear = 2000 + int(schoolYear[2:])       # "SY27" -> 2027
    return endYear + (12 - int(grade))


def readExport(source_folder):
    """
    Reads the half day report export.

    An ordinary CSV with a single header row. The file carries a UTF-8 BOM.
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
    required = ["Date", "Student > Name", "Student ID", "Student > Grade", "Code"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise SystemExit(
            Fore.YELLOW
            + f"Export is missing expected column(s): {', '.join(missing)}.\n"
            + f"Columns found: {', '.join(map(str, df.columns))}\n"
            + "ASPEN may have renamed a column, or this is the wrong report."
        )


def cleanData(df):
    entryID = getEntryID()
    week = getWeek()

    print("Cleaning data...")

    validateExport(df)

    df = df.rename(columns=COLUMN_MAPPINGS)

    # Normalise the date first: everything below depends on it.
    df["date"] = df["date"].apply(convertToStandardDate)
    df = df[df["date"].notna()].copy()

    # This export reaches back into the previous school year. Keep only the
    # current one -- the boundary is August, per _dataManager.getSchoolYear.
    currentSY = getSchoolYear(pd.Timestamp.now().strftime("%Y-%m-%d"))
    rowSY = df["date"].apply(getSchoolYear)
    keep = rowSY == currentSY
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

    # Derive YOG from grade and the current school year, never a fixed map.
    df["yog"] = df["grade"].apply(lambda g: gradeToYOG(g, currentSY)).astype("Int64")

    df = df.drop(columns=DROP_COLS, errors="ignore")

    df["entryID"] = int(entryID)
    df["week"] = str(week)
    df["name"] = df["name"].astype(str)
    df["code"] = df["code"].astype(str)
    df["excused"] = df["excused"].astype(bool)
    df["percentAbs"] = pd.to_numeric(df["percentAbs"], errors="coerce").astype(float)

    df = df[ORDERED_COLS]

    return df


def checkHalfDay(df, currentSY):
    """
    Reports anything odd. Advisory only -- no totals row in this export.
    """
    print(Fore.GREEN + "Checking the half day pull...")
    print(Fore.GREEN + f"  {len(df)} rows in {currentSY}, "
                       f"{df['id'].nunique()} distinct students, "
                       f"dates {df['date'].min()} to {df['date'].max()}.")
    print(Fore.GREEN + f"  codes: {df['code'].value_counts().to_dict()}")

    noYOG = int(df["yog"].isna().sum())
    if noYOG:
        print(Fore.YELLOW + f"  {noYOG} row(s) have no YOG (missing grade level).")


def doWork():
    """
    Main function for the script.
    """
    print(Style.BRIGHT + f"Starting {scriptName} script...")

    # Read in the export. The source file is never opened for writing.
    csv_file, rawDataFrame = readExport(source_folder)

    # Clean the data
    cleanedDataFrame = cleanData(rawDataFrame)

    checkHalfDay(cleanedDataFrame, getSchoolYear(pd.Timestamp.now().strftime("%Y-%m-%d")))

    min_date, max_date = cleanedDataFrame["date"].min(), cleanedDataFrame["date"].max()

    # This report is an event stream: the incoming date range is replaced.
    # ENABLED. THE DELETE AND THE UPLOAD ARE ONE OPERATION -- never comment out
    # one without the other, or this removes the date range and puts nothing back.
    deleteDataBetweenDates(project_id, dataset_id, table_id, min_date, max_date)
    uploadToBigQuery(cleanedDataFrame, tableSchema, project_id, dataset_id, table_id)

    # Archive the source file
    # NOTE: archiveSourceFile writes the cleaned frame to archivedFiles/ and then
    # DELETES the source export. Keep a spare copy while testing.
    archiveSourceFile(cleanedDataFrame, csv_file, source_folder, table_id)

    print(Style.BRIGHT + f"{scriptName} data pull complete.")


if __name__ == "__main__":
    doWork()
