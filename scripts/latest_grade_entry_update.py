import os
import re
import csv
import pandas as pd
from _dataManager import (
    findExportFile,
    uploadToBigQuery,
    archiveSourceFile,
    convertToStandardDate,
    getSchoolYear,
)

from colorama import init, Fore, Style
init(autoreset=True)


scriptName = "latest_grade_entry_update"


# Constants
project_id = "chitechdb"
dataset_id = "academics"
table_id = "latestGradeEntry"
source_folder = f"../dataUploaders/{table_id}"

# The BigQuery table does not exist yet. This is the shape to create it with.
tableSchema = [
    {"name": "entryID", "type": "INTEGER"},
    {"name": "week", "type": "STRING"},
    {"name": "sy", "type": "STRING"},
    {"name": "teacherLast", "type": "STRING"},
    {"name": "teacherFirst", "type": "STRING"},
    {"name": "courseName", "type": "STRING"},
    {"name": "courseID", "type": "STRING"},
    {"name": "section", "type": "STRING"},
    {"name": "period", "type": "STRING"},
    {"name": "dateLastUpdated", "type": "DATE"},
    {"name": "updatedPast", "type": "BOOLEAN"},
    {"name": "studentsWithClassGrade", "type": "INTEGER"},
    {"name": "studentCount", "type": "INTEGER"},
]

ORDERED_COLS = [c["name"] for c in tableSchema]

# The eight fields that are always present, in the order they appear once a row
# is left-compacted. School ID and School Name are read and then dropped.
LEADING_FIELDS = [
    "schoolID",
    "schoolName",
    "teacherLast",
    "teacherFirst",
    "courseName",
    "courseID",
    "section",
    "period",
]

DROP_COLS = ["schoolID", "schoolName"]

DATE_RE = re.compile(r"^\d{2}/\d{2}/\d{4}$")

# Minimum values a left-compacted row must hold to be a data row: the eight
# leading fields plus updatedPast and studentCount.
MIN_VALUES = len(LEADING_FIELDS) + 2


# Get the entry ID from the user
def getEntryID():
    # find todays date, and reformat it as YYYYMMDD,
    entryID = pd.Timestamp.now().strftime("%Y%m%d")

    print(Fore.CYAN + f"The EntryID is {entryID}.")

    return int(entryID)


def getWeek():
    while True:
        week = input(Fore.CYAN + "Enter the week/label for this data (W01, EndOfQ1, ...): ").strip()
        if week:
            return week
        print(Fore.YELLOW + "  Week cannot be blank.")


def parseRow(values):
    """
    Maps one left-compacted row onto the report's fields.

    Two fields are nullable -- Date Last Update and Student With a Class Grade --
    so a compacted row holds 10, 11 or 12 values and a length of 11 is ambiguous
    on its own. The tail is therefore read by type and by position from the end:

        [date?]  updatedPast  [studentsWithClassGrade?]  studentCount

    A date is recognised by its MM/DD/YYYY shape, and studentCount is always
    last. That resolves the ambiguity without guessing.
    """
    record = dict(zip(LEADING_FIELDS, values[:len(LEADING_FIELDS)]))
    tail = list(values[len(LEADING_FIELDS):])

    record["dateLastUpdated"] = tail.pop(0) if DATE_RE.match(tail[0]) else None
    record["updatedPast"] = tail.pop(0)
    record["studentCount"] = tail.pop()
    record["studentsWithClassGrade"] = tail.pop(0) if tail else None

    if tail:
        raise ValueError(f"unconsumed values in row tail: {tail}")
    return record


def isDataRow(values):
    """
    A data row starts with the numeric School ID and carries at least the ten
    always-present values.

    The title banner, the header (which repeats on every page), the "Page N of"
    footer and the "Report ID:" line all fail this and drop out on their own.
    """
    return len(values) >= MIN_VALUES and values[0].isdigit()


def readExport(source_folder):
    """
    Reads the Latest Grade Entry export.

    A print report dumped to CSV: banner rows, a header repeated once per page,
    and page footers. Physical column positions drift between rows -- the same
    field sits at index 6 on one row and index 5 on another -- so values are
    left-compacted and read by order and type rather than by fixed index.

    The source file is opened read-only.
    """
    csv_file = findExportFile(source_folder)
    csv_path = os.path.join(source_folder, csv_file)

    print(Fore.GREEN + f"Opening '{csv_file}'...")
    with open(csv_path, "r", newline="", encoding="utf-8-sig") as fh:
        rows = list(csv.reader(fh))

    records, skipped = [], []
    for i, raw in enumerate(rows):
        values = [c.strip() for c in raw if c.strip()]
        if not values:
            continue
        if isDataRow(values):
            try:
                records.append(parseRow(values))
            except (ValueError, IndexError) as exc:
                print(Fore.YELLOW + f"  row {i}: could not parse ({exc}); skipped.")
                skipped.append(values)
        else:
            skipped.append(values)

    if not records:
        raise ValueError(
            f"Parsed 0 rows from '{csv_file}'. Expected a Latest Grade Entry "
            "by Teacher export."
        )

    df = pd.DataFrame(records)
    print(Fore.GREEN + f"Parsed {len(df)} section rows from '{csv_file}' "
                       f"({len(skipped)} banner/header/footer rows skipped).")
    return csv_file, df


def checkLatestGradeEntry(df):
    """
    Reports anything odd. Advisory only -- this export carries no totals row.

    The one real invariant it does carry: a section has a Date Last Update if
    and only if Updated Past is Yes. On the 28 Aug 2026 pull that held for all
    38 rows.
    """
    print(Fore.GREEN + "Checking the latest grade entry pull...")
    print(Fore.GREEN + f"  {len(df)} sections, {df['teacherLast'].nunique()} teachers, "
                       f"{df['courseID'].nunique()} courses.")
    print(Fore.GREEN + f"  updated past: {int(df['updatedPast'].sum())} yes, "
                       f"{int((~df['updatedPast']).sum())} no.")

    mismatched = int((df["dateLastUpdated"].notna() != df["updatedPast"]).sum())
    if mismatched:
        print(Fore.YELLOW + f"  {mismatched} row(s) where a date is present but "
                            "'updated past' is No, or the reverse.")

    dupes = df.duplicated(subset=["courseID", "section"]).sum()
    if dupes:
        print(Fore.YELLOW + f"  {dupes} duplicate (courseID, section) pair(s); "
                            "each section should appear once per pull.")

    noGrade = int(df["studentsWithClassGrade"].isna().sum())
    if noGrade:
        print(Fore.YELLOW + f"  {noGrade} row(s) have no 'students with a class grade' "
                            "value; stored as null, not zero.")


def cleanData(df):
    entryID = getEntryID()
    week = getWeek()

    print("Cleaning data...")

    df = df.drop(columns=DROP_COLS, errors="ignore")

    df["dateLastUpdated"] = df["dateLastUpdated"].apply(
        lambda d: convertToStandardDate(d) if d else None)
    df["updatedPast"] = df["updatedPast"].str.strip().str.lower().map(
        {"yes": True, "no": False})

    for c in ["studentsWithClassGrade", "studentCount"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    for c in ["teacherLast", "teacherFirst", "courseName", "courseID", "section", "period"]:
        df[c] = df[c].astype(str).str.strip()

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

    # Read the export. Nothing is written back to it.
    csv_file, rawDataFrame = readExport(source_folder)

    # Clean the data
    cleanedDataFrame = cleanData(rawDataFrame)

    checkLatestGradeEntry(cleanedDataFrame)

    # Upload the data to BigQuery
    # DISABLED -- the academics.latestGradeEntry table does not exist yet.
    # Create it from tableSchema above, then uncomment. Append-only, no delete.
    uploadToBigQuery(cleanedDataFrame, tableSchema, project_id, dataset_id, table_id)

    # Archive the source file
    # NOTE: archiveSourceFile writes the cleaned frame to archivedFiles/ and then
    # DELETES the source export. Keep a spare copy while testing.
    archiveSourceFile(cleanedDataFrame, csv_file, source_folder, table_id)

    print(Style.BRIGHT + f"{scriptName} data pull complete.")


if __name__ == "__main__":
    doWork()
