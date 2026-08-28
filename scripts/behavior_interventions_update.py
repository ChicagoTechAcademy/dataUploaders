import os
import re
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


scriptName = "Branching Minds Interventions"


# Constants
project_id = "chitechdb"
dataset_id = "behavior"
table_id = "interventions"
source_folder = f"../dataUploaders/branchingMindsDownloads/{table_id}"

# The key selectNewRows anti-joins on. Branching Minds ships its full history
# every pull, so only unseen rows are appended.
KEY = "interventionPlanID"

tableSchema = [
    {"name": "entryID", "type": "INTEGER"},
    {"name": "interventionPlanID", "type": "STRING"},
    {"name": "id", "type": "INTEGER"},
    {"name": "sy", "type": "STRING"},
    {"name": "gradeLevel", "type": "STRING"},
    {"name": "interventionName", "type": "STRING"},
    {"name": "subjectArea", "type": "STRING"},
    {"name": "supportType", "type": "STRING"},
    {"name": "supportUsedAs", "type": "STRING"},
    {"name": "applicableTiers", "type": "STRING"},
    {"name": "interventionFrequency", "type": "STRING"},
    {"name": "durationPerOccurrence", "type": "STRING"},
    {"name": "totalMinutesPlanned", "type": "INTEGER"},
    {"name": "totalMinutesDelivered", "type": "INTEGER"},
    {"name": "interventionStartDate", "type": "DATE"},
    {"name": "interventionEndDate", "type": "DATE"},
    {"name": "interventionSetting", "type": "STRING"},
    {"name": "staffDelivering", "type": "STRING"},
]

ORDERED_COLS = [c["name"] for c in tableSchema]

COLUMN_MAPPING = {
    "Intervention Plan ID": "interventionPlanID",
    "Student ID": "id",
    "Grade Level": "gradeLevel",
    "Intervention Name": "interventionName",
    "Intervention Subject Area": "subjectArea",
    "Support Type": "supportType",
    "Support Used As": "supportUsedAs",
    "Applicable Tiers": "applicableTiers",
    "Intervention Frequency": "interventionFrequency",
    "Duration per Occurrence": "durationPerOccurrence",
    "Total Minutes Planned": "totalMinutesPlanned",
    "Total Minutes Delivered": "totalMinutesDelivered",
    "Intervention Start Date": "interventionStartDate",
    "Intervention End Date": "interventionEndDate",
    "Intervention Setting": "interventionSetting",
    "Staff Delivering Intervention": "staffDelivering",
}

# NOTE -- dropped columns, and why:
#   Student Name, State ID ......... not stored; `id` is the key.
#   The six demographic columns .... studentFlags is sourced from ASPEN.
#   School Name, School ID, District  single-valued; one school.
#   Support Evidence Level, Support Tags, Technology Required, Support Cost,
#   Staff Email .................... catalogue detail about the intervention
#                                    programme rather than about this student's
#                                    plan; add back if they get used.
#
# `sy` comes from the intervention START date -- a plan can run across a year
# boundary, and the start is when it was decided.

DATE_COLS = ["interventionStartDate", "interventionEndDate"]

# "Applicable Tiers" arrives as a JSON array string: ["2","3"]. Stored as a
# plain comma-separated string so it reads as 1,2,3 rather than JSON.
def tiersToString(value):
    text = (value or "").strip()
    if not text:
        return None
    parts = re.findall(r"\d+", text)
    return ",".join(sorted(set(parts))) if parts else None



def getEntryID():
    entryID = pd.Timestamp.now().strftime("%Y%m%d")
    print(Fore.CYAN + f"The EntryID is {entryID}.")
    return int(entryID)


def readExport(source_folder):
    """
    Reads the Branching Minds intervention export.

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
            "Expected the intervention_report_with_demographics_export export."
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

    df["applicableTiers"] = df["applicableTiers"].apply(tiersToString)

    for c in ["id", "totalMinutesPlanned", "totalMinutesDelivered"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    df["sy"] = [getSchoolYear(d.strftime("%Y-%m-%d")) if pd.notna(d) else None
                for d in df["interventionStartDate"]]

    df["entryID"] = int(entryID)

    df = df.replace({"": None})
    df = df[ORDERED_COLS]

    return df


def checkInterventions(df):
    """
    Reports anything odd. Advisory only -- this export carries no totals row.
    """
    print(Fore.GREEN + "Checking the intervention pull...")
    print(Fore.GREEN + f"  {len(df)} intervention plans, {df['id'].nunique()} students.")
    print(Fore.GREEN + f"  subjects: {dict(df['subjectArea'].value_counts())}")
    print(Fore.GREEN + f"  support types: {dict(df['supportType'].value_counts())}")

    short = int((df["totalMinutesDelivered"] < df["totalMinutesPlanned"]).sum())
    if short:
        print(Fore.YELLOW + f"  {short} plan(s) delivered fewer minutes than planned.")

    bad = int(df["interventionStartDate"].isna().sum())
    if bad:
        print(Fore.YELLOW + f"  {bad} row(s) have an unparseable start date.")

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

    checkInterventions(cleanedDataFrame)

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
