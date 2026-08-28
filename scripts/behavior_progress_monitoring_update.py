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
    getSchoolYear,
)

from colorama import init, Fore, Style
init(autoreset=True)


scriptName = "Branching Minds Behavior Progress Monitoring"


# Constants
project_id = "chitechdb"
dataset_id = "behavior"
table_id = "progressMonitoring"
source_folder = f"../dataUploaders/branchingMindsDownloads/{table_id}"

# No unique id in this export either: Goal ID and Plan ID both repeat (7 distinct
# across 27 rows) because one goal is scored many times. Goal plus score date
# identifies a row -- 27 of 27 distinct in the first pull.
KEY = "rowKey"
KEY_PARTS = ["goalID", "scoreDate"]

tableSchema = [
    {"name": "entryID", "type": "INTEGER"},
    {"name": "rowKey", "type": "STRING"},
    {"name": "goalID", "type": "STRING"},
    {"name": "planID", "type": "STRING"},
    {"name": "id", "type": "INTEGER"},
    {"name": "sy", "type": "STRING"},
    {"name": "gradeLevel", "type": "STRING"},
    {"name": "topic", "type": "STRING"},
    {"name": "behaviorProgressMonitor", "type": "STRING"},
    {"name": "behaviorMeasure", "type": "STRING"},
    {"name": "goalScore", "type": "FLOAT"},
    {"name": "goalDescription", "type": "STRING"},
    {"name": "startDate", "type": "DATE"},
    {"name": "endDate", "type": "DATE"},
    {"name": "scoreDate", "type": "DATE"},
    {"name": "score", "type": "FLOAT"},
    {"name": "periodName", "type": "STRING"},
    {"name": "monitoringStaff", "type": "STRING"},
    {"name": "reportingStaff", "type": "STRING"},
    {"name": "planStatus", "type": "STRING"},
    {"name": "notes", "type": "STRING"},
]

ORDERED_COLS = [c["name"] for c in tableSchema]

# This export uses different header names from every other Branching Minds file:
# "Grade" not "Grade Level", "Student ID" is present but the name is split into
# "Student First Name" / "Student Last Name".
COLUMN_MAPPING = {
    "Goal ID": "goalID",
    "Plan ID": "planID",
    "Student ID": "id",
    "Grade": "gradeLevel",
    "Topic": "topic",
    "Behavior Progress Monitor": "behaviorProgressMonitor",
    "Behavior Measure": "behaviorMeasure",
    "Goal Score": "goalScore",
    "Goal Description": "goalDescription",
    "Start Date": "startDate",
    "End Date": "endDate",
    "Score Date": "scoreDate",
    "Score": "score",
    "Period Name": "periodName",
    "Monitoring Staff": "monitoringStaff",
    "Reporting Staff": "reportingStaff",
    "Plan Status": "planStatus",
    "Notes": "notes",
}

# NOTE -- dropped columns, and why:
#   Student First Name, Student Last Name, Student State ID ... not stored.
#   Gender, Race/Ethnicity, English Proficiency, Gifted Program,
#   Mandated Learning Plan, ELL Status ........................ studentFlags is
#                                                               sourced from ASPEN.
#   Network, School ID, School Name, District, District State . single-valued.
#   School Year ............................................... the export's own
#       label is not used; `sy` is derived from the score date so it follows the
#       warehouse's August rollover rather than the vendor's convention.

DATE_COLS = ["startDate", "endDate", "scoreDate"]


def getEntryID():
    entryID = pd.Timestamp.now().strftime("%Y%m%d")
    print(Fore.CYAN + f"The EntryID is {entryID}.")
    return int(entryID)


def readExport(source_folder):
    """
    Reads the Branching Minds behaviour progress monitoring export.

    Read with csv.reader, not by line: goal descriptions and notes are free text
    and carry commas and newlines inside their quotes.

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
            f"'{csv_file}' is missing expected column(s): {missing}. Expected the "
            "behavior_progress_monitoring_plans_and_scores export -- note it uses "
            "different header names from the other Branching Minds files."
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
    for c in ["goalScore", "score"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["rowKey"] = buildRowKey(df, KEY_PARTS)

    df["entryID"] = int(entryID)

    # School year from the score date -- a goal can span a year boundary, and the
    # score is the thing being recorded.
    df["sy"] = [getSchoolYear(d.strftime("%Y-%m-%d")) if pd.notna(d) else None
                for d in df["scoreDate"]]

    df = df.replace({"": None})
    df = df[ORDERED_COLS]

    return df


def checkProgressMonitoring(df):
    """
    Reports anything odd. Advisory only -- this export carries no totals row.
    """
    print(Fore.GREEN + "Checking the progress monitoring pull...")
    print(Fore.GREEN + f"  {len(df)} scores across {df['goalID'].nunique()} goals, "
                       f"{df['id'].nunique()} students.")
    print(Fore.GREEN + f"  plan status: {dict(df['planStatus'].value_counts())}")

    bad = int(df["scoreDate"].isna().sum())
    if bad:
        print(Fore.YELLOW + f"  {bad} row(s) have no usable score date, so their "
                            "rowKey and sy are built from a null.")

    met = int((df["score"] >= df["goalScore"]).sum())
    print(Fore.GREEN + f"  {met} of {len(df)} scores met or beat the goal.")

    dupes = int(df.duplicated(subset=[KEY]).sum())
    if dupes:
        print(Fore.YELLOW + f"  {dupes} row(s) share a rowKey WITHIN this export -- "
                            "same goal scored twice on one date. The anti-join "
                            "will load only one of them.")


def doWork():
    """
    Main function for the script.
    """
    print(Style.BRIGHT + f"Starting {scriptName} script...")

    # Read the export. Nothing is written back to it.
    csv_file, rawDataFrame = readExport(source_folder)

    # Clean the data. One row in, one row out -- nothing is merged or aggregated.
    cleanedDataFrame = cleanData(rawDataFrame)

    checkProgressMonitoring(cleanedDataFrame)

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
