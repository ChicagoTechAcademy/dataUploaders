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


scriptName = "Branching Minds Behavior Incidents"


# Constants
project_id = "chitechdb"
dataset_id = "behavior"
table_id = "behaviorIncidents"
source_folder = f"../dataUploaders/branchingMindsDownloads/{table_id}"

# The unique row key. Every load is an anti-join against the values already in
# the warehouse, so re-exporting the full history only ever appends what is new.
KEY = "brmDetailID"

tableSchema = [
    {"name": "entryID", "type": "INTEGER"},
    {"name": "brmDetailID", "type": "STRING"},
    {"name": "brmIncidentID", "type": "STRING"},
    {"name": "id", "type": "INTEGER"},
    {"name": "sy", "type": "STRING"},
    {"name": "gradeLevel", "type": "STRING"},
    {"name": "incidentDate", "type": "DATE"},
    {"name": "incidentTime", "type": "TIME"},
    {"name": "studentRole", "type": "STRING"},
    {"name": "incidentCode", "type": "STRING"},
    {"name": "incidentLocation", "type": "STRING"},
    {"name": "actionTaken", "type": "STRING"},
    {"name": "reportingStaff", "type": "STRING"},
    {"name": "reportedDate", "type": "DATE"},
]

ORDERED_COLS = [c["name"] for c in tableSchema]

COLUMN_MAPPING = {
    "BRM Detail ID": "brmDetailID",
    "BRM Incident ID": "brmIncidentID",
    "Student ID": "id",
    "Grade Level": "gradeLevel",
    "Date": "incidentDate",
    "Time": "incidentTime",
    "Student Role": "studentRole",
    "Incident Type": "incidentCode",
    "Incident Location": "incidentLocation",
    "Action Taken": "actionTaken",
    "Reporting Staff": "reportingStaff",
    "Reported Date": "reportedDate",
}

# NOTE -- the export has 37 columns; 12 are read and 25 dropped.
#
#   brmDetailID is kept because it is the ONLY unique column in the export and
#   is therefore the only way to answer "is this incident already loaded".
#   Without it the anti-join below is impossible.
#
#   brmIncidentID is kept because one incident can involve several students:
#   2,533 student-rows come from 1,779 incidents. Without it "how many incidents
#   were there" cannot be answered from this table, only "how many students were
#   involved in one" -- two different numbers that both get reported.
#
#   Dropped: the full Incident Type policy text (300+ chars; the leading code is
#   extracted and the prose discarded), Incident Description, Antecedent,
#   Consequence, Student Account, both Deescalation Strategy fields, Restraint /
#   Seclusion / Family Notified / Police Notified, Instructional Time Lost,
#   Source Incident ID, Source Detail ID, Student Name, State ID, School Name,
#   School ID, District, Data Source, and the six demographic columns.
#
#   The demographics are NOT loaded from here. studentFlags is sourced from
#   ASPEN as a separate export -- Branching Minds is not the system of record
#   for them.
#
#   Grade Level IS kept: it is the student's grade WHEN THE INCIDENT HAPPENED,
#   which the current roster cannot reconstruct.

# "1.2 Engaging in behavior that interferes with..." -- keep the code, drop the
# paragraph.
CODE_RE = re.compile(r"^(\d+\.\d+)")


def getEntryID():
    entryID = pd.Timestamp.now().strftime("%Y%m%d")
    print(Fore.CYAN + f"The EntryID is {entryID}.")
    return int(entryID)


def readExport(source_folder):
    """
    Reads the Branching Minds behaviour incident export.

    Read with csv.reader, not by line: the incident type carries the entire
    policy paragraph, commas and all, and several free-text fields hold newlines.

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
            "new_behavior_incident_report_w_demographics export -- the OLD "
            "behavior_incident_report_export has different, snake_case headers."
        )

    df = pd.DataFrame(rows[1:], columns=header)
    print(Fore.GREEN + f"Parsed {len(df)} incident rows from '{csv_file}'.")
    return csv_file, df


def cleanData(df):
    entryID = getEntryID()

    print("Cleaning data...")

    for c in df.columns:
        df[c] = df[c].astype(str).str.replace(r"[ \t]+", " ", regex=True).str.strip()

    df = df[list(COLUMN_MAPPING)].rename(columns=COLUMN_MAPPING)

    # Dates arrive as M/D/YY. Parsed explicitly rather than left to inference,
    # which reads 1/12/26 differently depending on locale.
    df["incidentDate"] = parseFlexibleDate(df["incidentDate"])
    df["reportedDate"] = parseFlexibleDate(df["reportedDate"])
    df["incidentTime"] = pd.to_datetime(df["incidentTime"], format="%I:%M %p",
                                        errors="coerce").dt.time

    df["incidentCode"] = df["incidentCode"].str.extract(CODE_RE)[0]

    df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
    df["entryID"] = int(entryID)

    # School year from the incident date, not from today: this export carries
    # more than one year in a single file, and entryID is the pull date.
    df["sy"] = [getSchoolYear(d.strftime("%Y-%m-%d")) if pd.notna(d) else None
                for d in df["incidentDate"]]

    df = df.replace({"": None})
    df = df[ORDERED_COLS]

    return df


def checkBehaviorIncidents(df):
    """
    Reports anything odd. Advisory only -- this export carries no totals row.
    """
    print(Fore.GREEN + "Checking the behaviour incident pull...")
    print(Fore.GREEN + f"  {len(df)} student-incident rows from "
                       f"{df['brmIncidentID'].nunique()} distinct incidents, "
                       f"{df['id'].nunique()} students.")
    print(Fore.GREEN + f"  {df['incidentCode'].nunique()} incident codes, dates "
                       f"{df['incidentDate'].min()} to {df['incidentDate'].max()}.")
    print(Fore.GREEN + f"  school years: {sorted(df['sy'].dropna().unique())}")

    dupes = int(df.duplicated(subset=[KEY]).sum())
    if dupes:
        print(Fore.YELLOW + f"  {dupes} duplicate {KEY} value(s) WITHIN this export. "
                            "That column is supposed to be unique; the anti-join "
                            "cannot protect against duplicates inside one file.")

    badDate = int(df["incidentDate"].isna().sum())
    if badDate:
        print(Fore.YELLOW + f"  {badDate} row(s) have an unparseable incident date.")

    noCode = int(df["incidentCode"].isna().sum())
    if noCode:
        print(Fore.YELLOW + f"  {noCode} row(s) have an incident type with no leading "
                            "code; the code is null and the text is not stored.")

    noId = int(df["id"].isna().sum())
    if noId:
        print(Fore.YELLOW + f"  {noId} row(s) have no usable student id.")

    noAction = int(df["actionTaken"].isna().sum())
    if noAction:
        print(Fore.YELLOW + f"  {noAction} row(s) record no action taken.")


def doWork():
    """
    Main function for the script.
    """
    print(Style.BRIGHT + f"Starting {scriptName} script...")

    # Read the export. Nothing is written back to it.
    csv_file, rawDataFrame = readExport(source_folder)

    # Clean the data. One row in, one row out -- nothing is merged or aggregated.
    cleanedDataFrame = cleanData(rawDataFrame)

    checkBehaviorIncidents(cleanedDataFrame)

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
