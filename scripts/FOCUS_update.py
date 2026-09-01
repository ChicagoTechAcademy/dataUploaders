import os
import re
import csv
import pandas as pd
from _dataManager import (
    findExportFile,
    deleteDataBetweenDates,
    uploadToBigQuery,
    archiveSourceFile,
    getSchoolYear,
)

from colorama import init, Fore, Style
init(autoreset=True)


scriptName = "FOCUS Swipes"


# Constants
project_id = "chitechdb"
dataset_id = "attendance"
table_id = "FOCUS"
source_folder = f"../dataUploaders/{table_id}"

tableSchema = [
    {"name": "entryID", "type": "INTEGER"},
    {"name": "id", "type": "INTEGER"},
    {"name": "name", "type": "STRING"},
    {"name": "status", "type": "STRING"},
    {"name": "period", "type": "STRING"},
    {"name": "date", "type": "DATE"},
    {"name": "time", "type": "TIME"},
    {"name": "sy", "type": "STRING"},
]

ORDERED_COLS = [c["name"] for c in tableSchema]

COLUMN_MAPPING = {
    "Swipe_Time": "swipeTime",
    "Student_ID": "id",
    "Student_Name": "name",
    "Status": "status",
    "Period_Cause": "period",
}

# NOTE -- dropped columns, and why:
#   Division ..... the student's homeroom at the time of the swipe. Dropped by
#                  request; reachable for the current year through
#                  studentInfo.roster -> schedules.homerooms.
#   Row_Number ... a 1..N counter within this export. Used below as a checksum,
#                  then discarded -- it restarts at 1 on every pull, so it is
#                  not an identifier.
#   Textbox6 ..... a running per-student tardy count as of the report. It is a
#                  derived figure that goes stale the moment another swipe lands,
#                  and can be recomputed from this table at any time.

HEADER_FIRST_COL = "Swipe_Time"

# Line 2 of the export carries both the range it covers and its own row count:
#   Chicago Tech Academy,8/24/2026 To 9/1/2026,Total Swipes on Report: 1133
RANGE_RE = re.compile(r"(\d{1,2}/\d{1,2}/\d{4})\s+To\s+(\d{1,2}/\d{1,2}/\d{4})")
TOTAL_RE = re.compile(r"Total Swipes on Report:\s*(\d+)")


def getEntryID():
    entryID = pd.Timestamp.now().strftime("%Y%m%d")
    print(Fore.CYAN + f"The EntryID is {entryID}.")
    return int(entryID)


def readExport(source_folder):
    """
    Reads the FOCUS swipes export.

    The file opens with two banner lines and a blank line before the real
    header. The previous version handled that by REWRITING the export in place
    to delete the first three lines, which was not idempotent -- a second run
    ate the header row too -- and broke the rule that an export is never opened
    for writing. The header is now located by name instead.

    Returns (filename, DataFrame, statedRange, statedTotal).
    """
    csv_file = findExportFile(source_folder)
    csv_path = os.path.join(source_folder, csv_file)

    print(Fore.GREEN + f"Opening '{csv_file}'...")
    with open(csv_path, "r", newline="", encoding="utf-8-sig") as fh:
        rows = list(csv.reader(fh))

    banner = " ".join(c for r in rows[:3] for c in r)
    rangeMatch = RANGE_RE.search(banner)
    totalMatch = TOTAL_RE.search(banner)

    statedRange = None
    if rangeMatch:
        statedRange = (
            pd.to_datetime(rangeMatch.group(1), format="%m/%d/%Y").date(),
            pd.to_datetime(rangeMatch.group(2), format="%m/%d/%Y").date(),
        )
    statedTotal = int(totalMatch.group(1)) if totalMatch else None

    headerIdx = next(
        (i for i, r in enumerate(rows) if r and r[0].strip() == HEADER_FIRST_COL), None)
    if headerIdx is None:
        raise ValueError(
            f"No '{HEADER_FIRST_COL}' header row in '{csv_file}'. Expected a "
            "Verify.net BA Swipes List export."
        )

    header = [h.strip() for h in rows[headerIdx]]
    missing = [h for h in COLUMN_MAPPING if h not in header]
    if missing:
        raise ValueError(f"'{csv_file}' is missing expected column(s): {missing}.")

    # Trailing blank lines are ordinary here -- the file ends with one.
    data = [r for r in rows[headerIdx + 1:] if r and any(c.strip() for c in r)]
    df = pd.DataFrame(data, columns=header)

    print(Fore.GREEN + f"Parsed {len(df)} swipe rows from '{csv_file}'.")
    if statedRange:
        print(Fore.GREEN + f"  the report covers {statedRange[0]} to {statedRange[1]}.")
    return csv_file, df, statedRange, statedTotal


def cleanData(df):
    entryID = getEntryID()

    print("Cleaning data...")

    for c in df.columns:
        df[c] = df[c].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()

    df = df[list(COLUMN_MAPPING)].rename(columns=COLUMN_MAPPING)

    # "8/25/2026 8:18:58 AM" -- given explicitly rather than inferred, so a
    # US-format date is never read as day-first.
    stamp = pd.to_datetime(df["swipeTime"], format="%m/%d/%Y %I:%M:%S %p",
                           errors="coerce")
    df["date"] = stamp.dt.date
    df["time"] = stamp.dt.time
    df = df.drop(columns=["swipeTime"])

    # A blank Period_Cause is normal: an arrival swipe has no period attached.
    # Stored as null rather than the string "nan".
    df["period"] = df["period"].replace("", None)

    df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")

    # School year per row from the swipe date, not from today.
    df["sy"] = [getSchoolYear(d.strftime("%Y-%m-%d")) if pd.notna(d) else None
                for d in df["date"]]

    df["entryID"] = int(entryID)

    df = df.replace({"": None})
    df = df[ORDERED_COLS]

    return df


def reconcile(df, rawDf, statedTotal):
    """
    Checks the parse against the two counts the export carries itself.

    The banner states "Total Swipes on Report: N", and Row_Number runs 1..N
    without gaps. Both are real checksums and neither was previously used.
    Returns True when the parse is trustworthy.
    """
    print(Fore.GREEN + "Reconciling against the report's own counts...")
    ok = True

    if statedTotal is None:
        print(Fore.YELLOW + "  no 'Total Swipes on Report' in the banner; cannot "
                            "reconcile on the stated total.")
        ok = False
    else:
        matched = len(df) == statedTotal
        ok &= matched
        print((Fore.GREEN if matched else Fore.YELLOW)
              + f"  parsed {len(df)} rows, report states {statedTotal}"
              + ("  MATCH" if matched else "  MISMATCH"))

    if "Row_Number" in rawDf.columns:
        nums = pd.to_numeric(rawDf["Row_Number"], errors="coerce").dropna().astype(int)
        expected = set(range(1, len(rawDf) + 1))
        gaps = sorted(expected - set(nums))
        if gaps:
            ok = False
            print(Fore.YELLOW + f"  Row_Number is not a complete 1..{len(rawDf)} "
                                f"sequence; {len(gaps)} missing, first few {gaps[:5]}")
        else:
            print(Fore.GREEN + f"  Row_Number runs 1..{len(rawDf)} with no gaps  MATCH")

    badStamp = int(df["date"].isna().sum())
    if badStamp:
        ok = False
        print(Fore.YELLOW + f"  {badStamp} row(s) have an unparseable swipe time.")

    if not ok:
        print(Fore.YELLOW + "Reconciliation FAILED.")
    return ok


def checkFOCUS(df):
    """
    Reports anything odd. Advisory only.
    """
    print(Fore.GREEN + "Checking the FOCUS pull...")
    print(Fore.GREEN + f"  {len(df)} swipes, {df['id'].nunique()} students, "
                       f"{df['date'].nunique()} school days.")
    print(Fore.GREEN + f"  status: {dict(df['status'].value_counts())}")
    print(Fore.GREEN + f"  school years: {sorted(df['sy'].dropna().unique())}")

    noPeriod = int(df["period"].isna().sum())
    print(Fore.GREEN + f"  {noPeriod} row(s) have no period -- expected for arrival "
                       "swipes.")

    noId = int(df["id"].isna().sum())
    if noId:
        print(Fore.YELLOW + f"  {noId} row(s) have no usable student id.")

    if df["sy"].nunique() > 1:
        print(Fore.YELLOW + "  this export spans more than one school year.")


def doWork():
    """
    Main function for the script.
    """
    print(Style.BRIGHT + f"Starting {scriptName} script...")

    # Read the export. Nothing is written back to it.
    csv_file, rawDataFrame, statedRange, statedTotal = readExport(source_folder)

    # Clean the data
    cleanedDataFrame = cleanData(rawDataFrame.copy())

    # Verify the parse against the report's own counts before writing.
    if not reconcile(cleanedDataFrame, rawDataFrame, statedTotal):
        print(Fore.YELLOW + "Aborting: nothing uploaded, source file left in place.")
        raise SystemExit(1)

    checkFOCUS(cleanedDataFrame)

    if statedRange is None:
        print(Fore.YELLOW + "Aborting: the banner gives no date range, so the rows "
                            "to replace cannot be determined.")
        raise SystemExit(1)

    # Replace the range this report covers, then insert. The delete and the
    # upload are ONE operation -- never enable one without the other, or a
    # failed run leaves a hole in the table.
    # ENABLED 2026-09-01.
    deleteDataBetweenDates(project_id, dataset_id, table_id,
                           statedRange[0], statedRange[1])
    uploadToBigQuery(cleanedDataFrame, tableSchema, project_id, dataset_id, table_id)

    # Archive the source file
    # NOTE: archiveSourceFile writes the cleaned frame to archivedFiles/ and then
    # DELETES the source export.
    archiveSourceFile(cleanedDataFrame, csv_file, source_folder, table_id)

    print(Style.BRIGHT + f"{scriptName} data pull complete.")


if __name__ == "__main__":
    doWork()
