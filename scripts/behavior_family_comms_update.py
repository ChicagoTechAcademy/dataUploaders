import os
import csv
import pandas as pd
from _dataManager import (
    findExportFile,
    selectNewRows,
    parseFlexibleDate,
    uploadToBigQuery,
    archiveSourceFile,
    getSchoolYear,
)

from colorama import init, Fore, Style
init(autoreset=True)


scriptName = "Branching Minds Family Communications"


# Constants
project_id = "chitechdb"
dataset_id = "behavior"
table_id = "familyComms"
source_folder = f"../dataUploaders/branchingMindsDownloads/{table_id}"

# The key selectNewRows anti-joins on. Branching Minds ships its full history
# every pull, so only unseen rows are appended.
KEY = "familyCommunicationID"

tableSchema = [
    {"name": "entryID", "type": "INTEGER"},
    {"name": "familyCommunicationID", "type": "STRING"},
    {"name": "id", "type": "INTEGER"},
    {"name": "sy", "type": "STRING"},
    {"name": "gradeLevel", "type": "STRING"},
    {"name": "commDate", "type": "DATE"},
    {"name": "communicationMethod", "type": "STRING"},
    {"name": "subject", "type": "STRING"},
    {"name": "notes", "type": "STRING"},
    {"name": "staffName", "type": "STRING"},
    {"name": "staffEmail", "type": "STRING"},
]

ORDERED_COLS = [c["name"] for c in tableSchema]

COLUMN_MAPPING = {
    "Family Communication ID": "familyCommunicationID",
    "Student Number": "id",
    "Grade Level": "gradeLevel",
    "Date": "commDate",
    "Communication Method": "communicationMethod",
    "Subject": "subject",
    "Notes": "notes",
    "Staff Name": "staffName",
    "Staff Email": "staffEmail",
}

# NOTE -- dropped columns, and why:
#   Student Name, State ID ......... not stored; `id` is the key.
#   The six demographic columns .... studentFlags is sourced from ASPEN.
#   School Name, School ID, District  single-valued; one school.
#
# WARNING: Family Communication ID was redacted to 777777777 in the sample
# export, so its uniqueness is UNVERIFIED. If the real export repeats it, the
# anti-join will wrongly treat already-loaded rows as seen and skip new ones.
# The duplicate check below is the tripwire -- do not ignore it.

DATE_COLS = ["commDate"]


def getEntryID():
    entryID = pd.Timestamp.now().strftime("%Y%m%d")
    print(Fore.CYAN + f"The EntryID is {entryID}.")
    return int(entryID)


def readExport(source_folder):
    """
    Reads the Branching Minds family communication export.

    Read with csv.reader, not by line: free-text fields carry commas and
    newlines inside their quotes.

    The source file is opened read-only.
    """
    csv_file = findExportFile(source_folder)
    csv_path = os.path.join(source_folder, csv_file)

    print(Fore.GREEN + f"Opening '{csv_file}'...")
    with open(csv_path, "r", newline="", encoding="utf-8-sig") as fh:
        rows = list(csv.reader(fh))

    if not rows:
        raise ValueError(f"'{csv_file}' is empty.")

    header = [h.strip().lstrip("﻿") for h in rows[0]]
    missing = [h for h in COLUMN_MAPPING if h not in header]
    if missing:
        raise ValueError(
            f"'{csv_file}' is missing expected column(s): {missing}. "
            "Expected the family_comms_with_demographics_export export."
        )

    df = pd.DataFrame(rows[1:], columns=header)
    print(Fore.GREEN + f"Parsed {len(df)} rows from '{csv_file}'.")
    return csv_file, df


def cleanData(df):
    entryID = getEntryID()

    print("Cleaning data...")

    for c in df.columns:
        df[c] = df[c].astype(str).str.replace(r"[ \t]+", " ", regex=True).str.strip()

    df = df[list(COLUMN_MAPPING)].rename(columns=COLUMN_MAPPING)

    for c in DATE_COLS:
        df[c] = parseFlexibleDate(df[c])

    df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")

    df["sy"] = [getSchoolYear(d.strftime("%Y-%m-%d")) if pd.notna(d) else None
                for d in df["commDate"]]

    df["entryID"] = int(entryID)

    df = df.replace({"": None})
    df = df[ORDERED_COLS]

    return df


def checkFamilyComms(df):
    """
    Reports anything odd. Advisory only -- this export carries no totals row.
    """
    print(Fore.GREEN + "Checking the family communication pull...")
    print(Fore.GREEN + f"  {len(df)} communications, {df['id'].nunique()} students.")
    print(Fore.GREEN + f"  methods: {dict(df['communicationMethod'].value_counts().head())}")

    bad = int(df["commDate"].isna().sum())
    if bad:
        print(Fore.YELLOW + f"  {bad} row(s) have an unparseable date.")

    dupes = int(df.duplicated(subset=[KEY]).sum())
    if dupes:
        print(Fore.YELLOW + f"  {dupes} duplicate {KEY} value(s) WITHIN this export. "
                            "The anti-join cannot protect against duplicates "
                            "inside one file.")


def doWork():
    """
    Main function for the script.
    """
    print(Style.BRIGHT + f"Starting {scriptName} script...")

    # Read the export. Nothing is written back to it.
    csv_file, rawDataFrame = readExport(source_folder)

    # Clean the data. One row in, one row out -- nothing is merged or aggregated.
    cleanedDataFrame = cleanData(rawDataFrame)

    checkFamilyComms(cleanedDataFrame)

    # Append only what is not already in the warehouse.
    newRows = selectNewRows(cleanedDataFrame, KEY, dataset_id, table_id)

    # Upload the data to BigQuery
    # ENABLED 2026-08-28. Dataset and table created the same day.
    if len(newRows):
        uploadToBigQuery(newRows, tableSchema, project_id, dataset_id, table_id)

    # Archive the source file
    # NOTE: archiveSourceFile writes the frame to archivedFiles/ and then DELETES
    # the source export.
    archiveSourceFile(newRows, csv_file, source_folder, table_id)

    print(Style.BRIGHT + f"{scriptName} data pull complete.")


if __name__ == "__main__":
    doWork()
