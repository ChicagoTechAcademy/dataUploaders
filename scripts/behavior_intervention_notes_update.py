import os
import csv
import pandas as pd
from _dataManager import (
    findExportFile,
    selectNewRows,
    parseFlexibleDate,
    buildRowKey,
    uploadToBigQuery,
    archiveSourceFile,
)

from colorama import init, Fore, Style
init(autoreset=True)


scriptName = "Branching Minds Intervention Notes"


# Constants
project_id = "chitechdb"
dataset_id = "behavior"
table_id = "interventionNotes"
source_folder = f"../dataUploaders/branchingMindsDownloads/{table_id}"

# This export ships NO unique id. Intervention Plan ID identifies the plan, and
# a plan carries several notes -- 94 notes across 24 plans in the first pull. The
# key is therefore a hash of the three fields that together identify one note.
KEY = "rowKey"
KEY_PARTS = ["interventionPlanID", "notesDate", "notes"]

tableSchema = [
    {"name": "entryID", "type": "INTEGER"},
    {"name": "rowKey", "type": "STRING"},
    {"name": "interventionPlanID", "type": "STRING"},
    {"name": "notesDate", "type": "DATE"},
    {"name": "notes", "type": "STRING"},
]

ORDERED_COLS = [c["name"] for c in tableSchema]

COLUMN_MAPPING = {
    "Intervention Plan ID": "interventionPlanID",
    "Notes Date": "notesDate",
    "Notes": "notes",
}

# NOTE -- this export carries NO student id and NO demographics. It reaches a
# student only through interventionPlanID -> behavior.interventions.
#
# `district` is dropped: single-valued.
#
# There is no `sy`. The note date is the only date, and a note can be written
# about a plan that started in a previous year, so deriving a school year from it
# would be asserting something the export does not say.
#
# WARNING: because the key is a hash of the note text, EDITING a note in
# Branching Minds makes it read as a new row. The edited note will be appended
# alongside the original rather than replacing it.

# Unlike every other Branching Minds export, this one uses ISO dates
# (2025-10-20) rather than M/D/YY.
DATE_FORMAT = "%Y-%m-%d"


def getEntryID():
    entryID = pd.Timestamp.now().strftime("%Y%m%d")
    print(Fore.CYAN + f"The EntryID is {entryID}.")
    return int(entryID)


def readExport(source_folder):
    """
    Reads the Branching Minds intervention notes export.

    Read with csv.reader, not by line: notes are free text and carry commas and
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
            "Expected the intervention_notes_with_demographics_export export."
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

    df["notesDate"] = parseFlexibleDate(df["notesDate"])

    # Built before the blank-to-null pass so the hash is over consistent values.
    df["rowKey"] = buildRowKey(df, KEY_PARTS)

    df["entryID"] = int(entryID)

    df = df.replace({"": None})
    df = df[ORDERED_COLS]

    return df


def checkInterventionNotes(df):
    """
    Reports anything odd. Advisory only -- this export carries no totals row.
    """
    print(Fore.GREEN + "Checking the intervention notes pull...")
    print(Fore.GREEN + f"  {len(df)} notes across "
                       f"{df['interventionPlanID'].nunique()} intervention plans.")

    bad = int(df["notesDate"].isna().sum())
    if bad:
        print(Fore.YELLOW + f"  {bad} row(s) have an unparseable note date. This "
                            "export uses ISO dates, unlike the others.")

    empty = int(df["notes"].isna().sum())
    if empty:
        print(Fore.YELLOW + f"  {empty} note(s) are empty.")

    dupes = int(df.duplicated(subset=[KEY]).sum())
    if dupes:
        print(Fore.YELLOW + f"  {dupes} row(s) share a rowKey WITHIN this export -- "
                            "identical plan, date and text. The anti-join cannot "
                            "tell them apart and will load only one.")


def doWork():
    """
    Main function for the script.
    """
    print(Style.BRIGHT + f"Starting {scriptName} script...")

    # Read the export. Nothing is written back to it.
    csv_file, rawDataFrame = readExport(source_folder)

    # Clean the data. One row in, one row out -- nothing is merged or aggregated.
    cleanedDataFrame = cleanData(rawDataFrame)

    checkInterventionNotes(cleanedDataFrame)

    # Append only what is not already in the warehouse.
    newRows = selectNewRows(cleanedDataFrame, KEY, dataset_id, table_id)

    # Upload the data to BigQuery
    # ENABLED 2026-08-28. Dataset and table created the same day.
    if len(newRows):
        uploadToBigQuery(newRows, tableSchema, project_id, dataset_id, table_id)

    # Archive the source file
    archiveSourceFile(newRows, csv_file, source_folder, table_id)

    print(Style.BRIGHT + f"{scriptName} data pull complete.")


if __name__ == "__main__":
    doWork()
