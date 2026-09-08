import os
import re
import csv
import pandas as pd
from _dataManager import (
    findExportFile,
    fetchDataFromBigQuery,
    uploadToBigQuery,
    archiveSourceFile,
    parseFlexibleDate,
    getSchoolYear,
)

from colorama import init, Fore, Style
init(autoreset=True)


scriptName = "All Assignments and Grades by Student"


# Constants
project_id = "chitechdb"
dataset_id = "academics"
table_id = "AllAssignByStudent"
source_folder = f"../dataUploaders/{table_id}"

tableSchema = [
    {"name": "entryID", "type": "INTEGER"},
    {"name": "sy", "type": "STRING"},
    {"name": "term", "type": "STRING"},
    {"name": "id", "type": "INTEGER"},
    {"name": "studentLast", "type": "STRING"},
    {"name": "studentFirst", "type": "STRING"},
    {"name": "gradeLevel", "type": "STRING"},
    {"name": "classCode", "type": "STRING"},
    {"name": "className", "type": "STRING"},
    {"name": "teacherLast", "type": "STRING"},
    {"name": "teacherFirst", "type": "STRING"},
    {"name": "assignmentName", "type": "STRING"},
    {"name": "gbColumnName", "type": "STRING"},
    {"name": "visibility", "type": "STRING"},
    {"name": "score", "type": "FLOAT"},
    {"name": "scorePossible", "type": "FLOAT"},
    {"name": "categoryCode", "type": "STRING"},
    {"name": "categoryName", "type": "STRING"},
    {"name": "categoryWeight", "type": "FLOAT"},
    {"name": "assignedDate", "type": "DATE"},
    {"name": "assignmentDue", "type": "DATE"},
    {"name": "gradeEnteredOn", "type": "DATE"},
]

ORDERED_COLS = [c["name"] for c in tableSchema]

COLUMN_MAPPING = {
    "Grade Term": "term",
    "Student ID": "id",
    "Student Last Name": "studentLast",
    "Student First Name": "studentFirst",
    "Grade Level": "gradeLevel",
    "Class Number": "classCode",
    "Class Name": "className",
    "Teacher Last Name": "teacherLast",
    "Teacher First Name": "teacherFirst",
    "Assignment Name": "assignmentName",
    "GB Column Name": "gbColumnName",
    "Visibility": "visibility",
    "Score": "score",
    "Score Possible": "scorePossible",
    "Category Code": "categoryCode",
    "Category Name": "categoryName",
    "Category Weight": "categoryWeight",
    "Assigned Date": "assignedDate",
    "Assignment Due": "assignmentDue",
    "Grade entered on": "gradeEnteredOn",
}

# NOTE -- dropped columns:
#   School ID, School Name ... single-valued; this is one school.
#
# NOTE -- columns kept that look empty but are not:
#   categoryWeight is 0 on every row TODAY. That is expected, not dead -- it is
#   kept so it works the moment weighting is turned on.
#   gradeEnteredOn is blank wherever a teacher has not marked the assignment.
#   That blank IS the signal: it is the only direct measure of what is still
#   unmarked, as opposed to inferring it from a missing letter grade.

DATE_COLS = ["assignedDate", "assignmentDue", "gradeEnteredOn"]


def getEntryID():
    entryID = pd.Timestamp.now().strftime("%Y%m%d")
    print(Fore.CYAN + f"The EntryID is {entryID}.")
    return int(entryID)


def tidyCategoryName(value):
    """
    Cleans a category name without changing what it says.

    Staff invent their own category names, so this column is free text, and some
    arrive as lists with stray spacing: "Checkpoints , Quiz ," becomes
    "Checkpoints, Quiz". Only whitespace and a dangling separator are touched --
    the words are preserved exactly.

    These will be mapped onto Daily / Practice / Apply / Assessment / Capstone
    later. That mapping is NOT applied here: the raw name has to survive so the
    mapping can be built against what staff actually typed.
    """
    text = (value or "").strip()
    if not text:
        return None
    text = re.sub(r"\s*,\s*", ", ", text)      # normalise spacing around commas
    text = re.sub(r"(,\s*)+$", "", text)       # drop trailing separators
    return re.sub(r"\s+", " ", text).strip() or None


def readExport(source_folder):
    """
    Reads the All Assignments and Grades export.

    Read with csv.reader, not by line: assignment and category names are free
    text and carry commas inside their quotes -- "Checkpoints , Quiz ," is a
    single field, not three.

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
            f"'{csv_file}' is missing expected column(s): {missing}. Expected an "
            "All Assignments And Grades export from ASPEN."
        )

    data = [r for r in rows[1:] if r and any(c.strip() for c in r)]
    df = pd.DataFrame(data, columns=header)
    print(Fore.GREEN + f"Parsed {len(df)} assignment rows from '{csv_file}'.")
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
    for c in ["score", "scorePossible", "categoryWeight"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["categoryName"] = df["categoryName"].apply(tidyCategoryName)

    df["entryID"] = int(entryID)

    # School year from the assigned date, not from today: a pull taken in
    # August covers work assigned in the same year, but a late pull should not
    # relabel it.
    df["sy"] = [getSchoolYear(d.strftime("%Y-%m-%d")) if pd.notna(d) else None
                for d in df["assignedDate"]]

    df = df.replace({"": None})
    df = df[ORDERED_COLS]

    return df


def replaceTerms(df):
    """
    Clears the terms this file covers, so the load replaces rather than repeats.

    The export carries whichever terms exist so far -- Term 1 today, and either
    Term 2 alone or Terms 1 and 2 once the quarter turns. Deleting exactly the
    terms PRESENT IN THE FILE works either way and needs no assumption about
    which it will be.

    Returns the terms it cleared.
    """
    terms = sorted(t for t in df["term"].dropna().unique())
    if not terms:
        raise ValueError("No Grade Term values in the export; refusing to load.")

    quoted = ", ".join(f'"{t}"' for t in terms)
    print(Style.BRIGHT + Fore.BLUE + f"Clearing existing rows for: {', '.join(terms)}...")
    try:
        fetchDataFromBigQuery(
            f"DELETE FROM `{dataset_id}.{table_id}` WHERE term IN ({quoted})")
    except Exception as exc:
        print(Fore.YELLOW + f"  could not clear ({type(exc).__name__}); if the table "
                            "does not exist yet this is expected on a first load.")
    return terms


def checkAllAssign(df):
    """
    Reports anything odd. Advisory only -- this export carries no totals row.
    """
    print(Fore.GREEN + "Checking the assignment pull...")
    print(Fore.GREEN + f"  {len(df)} rows, {df['id'].nunique()} students, "
                       f"{df['classCode'].nunique()} class sections, "
                       f"{df['gbColumnName'].nunique()} gradebook columns.")
    print(Fore.GREEN + f"  terms: {sorted(df['term'].dropna().unique())}")

    ungraded = int(df["gradeEnteredOn"].isna().sum())
    print(Fore.GREEN + f"  {ungraded} row(s) have no 'grade entered on' date "
                       f"({ungraded / max(len(df), 1) * 100:.1f}%) -- not yet marked.")

    noScore = int(df["score"].isna().sum())
    if noScore != ungraded:
        print(Fore.YELLOW + f"  {noScore} row(s) have no score but {ungraded} have no "
                            "entry date; those two should usually agree.")

    overMax = int((df["score"] > df["scorePossible"]).sum())
    if overMax:
        print(Fore.YELLOW + f"  {overMax} row(s) score higher than the maximum.")

    noPossible = int((df["scorePossible"].isna() | (df["scorePossible"] == 0)).sum())
    if noPossible:
        print(Fore.YELLOW + f"  {noPossible} row(s) have no points possible; those "
                            "cannot contribute to a percentage.")

    badDate = int(df["assignedDate"].isna().sum())
    if badDate:
        print(Fore.YELLOW + f"  {badDate} row(s) have an unparseable assigned date, "
                            "so their sy is null.")

    noId = int(df["id"].isna().sum())
    if noId:
        print(Fore.YELLOW + f"  {noId} row(s) have no usable student id.")


def doWork():
    """
    Main function for the script.
    """
    print(Style.BRIGHT + f"Starting {scriptName} script...")

    # Read the export. Nothing is written back to it.
    csv_file, rawDataFrame = readExport(source_folder)

    # Clean the data
    cleanedDataFrame = cleanData(rawDataFrame)

    checkAllAssign(cleanedDataFrame)

    # Replace the terms this file covers. The delete and the upload are ONE
    # operation -- never enable one without the other, or a failed run leaves
    # the term empty.
    # ENABLED 2026-09-08. Table created from tableSchema above on the same day.
    replaceTerms(cleanedDataFrame)
    uploadToBigQuery(cleanedDataFrame, tableSchema, project_id, dataset_id, table_id)

    # Archive the source file
    # NOTE: archiveSourceFile writes the cleaned frame to archivedFiles/ and then
    # DELETES the source export.
    archiveSourceFile(cleanedDataFrame, csv_file, source_folder, table_id)

    print(Style.BRIGHT + f"{scriptName} data pull complete.")


if __name__ == "__main__":
    doWork()
