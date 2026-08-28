import re
import os
import csv
import pandas as pd
from _dataManager import (
    readPaginatedExport,
    parseTotalRow,
    findExportFile,
    uploadToBigQuery,
    archiveSourceFile,
    getSchoolYear,
)

from colorama import init, Fore, Style
init(autoreset=True)


scriptName = "grades_update"


# Constants
project_id = "chitechdb"
dataset_id = "academics"
table_id = "grades"
source_folder = f"../dataUploaders/{table_id}"

# Order matches academics.grades exactly. yog and teacher were added to the
# table on 2026-08-28 and land at positions 9 and 10; historical rows are NULL
# for both.
tableSchema = [
    {"name": "entryID", "type": "INTEGER"},
    {"name": "week", "type": "STRING"},
    {"name": "sy", "type": "STRING"},
    {"name": "id", "type": "INTEGER"},
    {"name": "name", "type": "STRING"},
    {"name": "course", "type": "STRING"},
    {"name": "gradePercent", "type": "FLOAT"},
    {"name": "letterGrade", "type": "STRING"},
    {"name": "yog", "type": "INTEGER"},
    {"name": "teacher", "type": "STRING"},
]

ORDERED_COLS = [
    "entryID",
    "week",
    "sy",
    "id",
    "name",
    "course",
    "gradePercent",
    "letterGrade",
    "yog",
    "teacher",
]

# The export's own header text -> our column name. Trailing colons are stripped
# by the shared parser, so "Student ID:" matches "Student ID".
#
# This report is paginated and its layout drifts: in a single file, most pages
# put "Overall Average" at column 9 and at least one page puts it at column 10.
# That drift is the "misplaced column" the runbook used to say to fix by hand,
# and reading each page's own header removes the need to touch the file at all.
FIELD_MAP = {
    "YOG": "yog",
    "Name": "name",
    "Student ID": "id",
    "Course": "course",
    "Teacher": "teacher",
    "Overall Average": "averageAndLetter",
}

# "87.4 B" -> percent and letter. A blank cell or an unexpected third token used
# to break a naive str.split(expand=True) mid-run; this reports instead.
GRADE_RE = re.compile(r"^\s*(?P<pct>-?\d+(?:\.\d+)?)\s+(?P<letter>[A-Z][+-]?)\s*$")


# Get the entry ID from the user
def getEntryID():
    # entryID = input(Fore.CYAN + "Enter the entry ID for this roster YYYYMMDDX: ")

    # find todays date, and reformat it as YYYYMMDD,
    entryID = pd.Timestamp.now().strftime("%Y%m%d")

    print(Fore.CYAN + f"The EntryID is {entryID}.")

    return int(entryID)


def getWeek():
    return input(Fore.CYAN + "Enter the week/label for this data (W01, EndOfQ1, ...): ")


def splitAverage(series):
    """
    Splits "87.4 B" into a numeric percent and a letter grade.

    Anything that does not match is reported and returned as nulls rather than
    raising, so one odd cell cannot take down a whole run.
    """
    extracted = series.astype(str).str.extract(GRADE_RE)
    bad = int(extracted["pct"].isna().sum())
    if bad:
        examples = series[extracted["pct"].isna()].astype(str).unique()[:5].tolist()
        print(Fore.YELLOW + f"{bad} grade cell(s) not in 'NN.N X' form; stored as null. "
                            f"Examples: {examples}")
    return pd.to_numeric(extracted["pct"], errors="coerce"), extracted["letter"]


def checkGrades(df, statedTotal):
    """
    Reconciles the parse against the report's own Total row.

    CONFIRMED 28 Aug 2026: the Total row counts DISTINCT STUDENTS, not rows.
    A real pull read 170 against 199 course-grade rows across exactly 170
    students. (The earlier 172-vs-201 puzzle was unsolvable only because every
    Student ID in that sample had been dummied to the same value.)

    Returns True when the parse matches.
    """
    print(Fore.GREEN + "Checking the grades pull...")
    students = int(df["id"].nunique())
    print(Fore.GREEN + f"  {len(df)} course-grade rows, {students} distinct students.")

    ok = True
    if statedTotal is None:
        print(Fore.YELLOW + "  No 'Total' row found in the export; cannot reconcile.")
        ok = False
    else:
        matched = abs(students - statedTotal) < 0.01
        line = f"  distinct students   parsed={students:>7}  report={statedTotal:>7.0f}"
        print((Fore.GREEN if matched else Fore.YELLOW) + line
              + ("  MATCH" if matched else "  MISMATCH"))
        ok &= matched

    nullGrades = int(df["gradePercent"].isna().sum())
    if nullGrades:
        print(Fore.YELLOW + f"  {nullGrades} row(s) have no usable grade.")

    if not ok:
        print(Fore.YELLOW + "Reconciliation FAILED: parsed students do not match the "
                            "report's own Total.")
    return ok


# Clean the data
def cleanData(df):
    entryID = getEntryID()
    week = getWeek()

    # Rows with a non-numeric Student ID are footer junk, not students.
    # Coerce rather than regex-matching the string form: a single blank cell in
    # the export turns the column to float64 and every id renders as "5555.0".
    id_num = pd.to_numeric(df["id"], errors="coerce")
    isStudent = id_num.notna()
    dropped = int((~isStudent).sum())
    if dropped:
        print(Fore.YELLOW + f"Dropping {dropped} row(s) whose Student ID is not numeric.")
    df = df[isStudent].copy()

    df["id"] = id_num[isStudent].astype("int64")
    df["yog"] = pd.to_numeric(df["yog"], errors="coerce").astype("Int64")
    df["gradePercent"], df["letterGrade"] = splitAverage(df["averageAndLetter"])

    df["entryID"] = int(entryID)
    df["week"] = week
    # Derive the school year rather than hardcoding it; see _dataManager.getSchoolYear
    df["sy"] = getSchoolYear(pd.Timestamp.now().strftime("%Y-%m-%d"))

    df = df[ORDERED_COLS]

    return df


def doWork():
    """
    Main function for the script.
    """
    print(Style.BRIGHT + f"Starting {scriptName} script...")

    # Read and repair the export in memory. Nothing is written back to it -- the
    # old prepareCSVFile() rewrote the source in place and could truncate it to
    # zero bytes if it raised mid-write.
    csv_file, rawDataFrame, _ = readPaginatedExport(source_folder, FIELD_MAP, {})

    # The report carries a trailing "Total" row; read it for the check below.
    with open(os.path.join(source_folder, csv_file), "r", newline="", encoding="utf-8") as fh:
        statedTotal = parseTotalRow(list(csv.reader(fh)))

    # Clean the data
    cleanedDataFrame = cleanData(rawDataFrame)

    # Verify the parse against the report's own Total before writing anything.
    # A failure aborts before both the upload and the archive -- archiveSourceFile
    # deletes the source export, so a bad parse must not reach it.
    if not checkGrades(cleanedDataFrame, statedTotal):
        print(Fore.YELLOW + "Aborting: nothing uploaded, source file left in place.")
        raise SystemExit(1)

    # Upload the data to BigQuery
    # DISABLED -- enable when you decide to pull.
    uploadToBigQuery(cleanedDataFrame, tableSchema, project_id, dataset_id, table_id)

    # Archive the source file
    # NOTE: archiveSourceFile writes the cleaned frame to archivedFiles/ and then
    # DELETES the source export. Keep a spare copy while testing.
    archiveSourceFile(cleanedDataFrame, csv_file, source_folder, table_id)

    print(Style.BRIGHT + f"{scriptName} data pull complete.")


if __name__ == "__main__":
    doWork()
