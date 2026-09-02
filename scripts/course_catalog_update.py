import os
import csv
import pandas as pd
from _dataManager import (
    findExportFile,
    deleteAllDataFromTable,
    uploadToBigQuery,
    archiveSourceFile,
)

from colorama import init, Fore, Style
init(autoreset=True)


scriptName = "CPS Course Catalog"


# Constants
project_id = "chitechdb"
dataset_id = "academics"
table_id = "cpsCourseCatalog"
source_folder = f"../dataUploaders/{table_id}"

# The most credit types any one course carries. Measured at 10 on the
# 2026-08-28 export; readExport raises if a future pull exceeds it rather than
# silently dropping the overflow.
MAX_CREDIT_TYPES = 10

CREDIT_COLS = [f"creditType{i}" for i in range(1, MAX_CREDIT_TYPES + 1)]

# The BigQuery table does not exist yet. This is the shape to create it with.
tableSchema = [
    {"name": "courseNumber", "type": "STRING"},
    {"name": "title", "type": "STRING"},
    {"name": "active", "type": "BOOLEAN"},
    {"name": "academicLevel", "type": "STRING"},
    {"name": "creditsEarned", "type": "FLOAT"},
    {"name": "isbeNumber", "type": "STRING"},
    {"name": "gradCreditRaw", "type": "STRING"},
] + [{"name": c, "type": "STRING"} for c in CREDIT_COLS]

ORDERED_COLS = [c["name"] for c in tableSchema]

# Source header -> warehouse column. Every other column in the export is
# dropped; see NOTE below for why each one goes.
COLUMN_MAPPING = {
    "Course Number": "courseNumber",
    "Title": "title",
    "Active": "active",
    "Academic Level": "academicLevel",
    "Number of Credits Earned": "creditsEarned",
    "ISBE Number": "isbeNumber",
    "Type of Graduation Credit Earned": "gradCreditRaw",
}

# NOTE -- dropped columns, and why:
#   Content Team, Second Content Team ..... byte-identical copies of Primary /
#                                           Secondary Content Team (all rows).
#   Part of Sequence ...................... equals Course Number on 5,744 of
#                                           5,747 populated rows; carries nothing.
#   Term in Sequence, Career Cluster ...... 100% empty.
#   Possible College Credit Hours ......... only non-blank value is "N/A".
#   ISBE Source ........................... corrupt: holds the ISBE title on
#                                           some rows and the ISBE number on
#                                           others. ISBE Number is clean.
#   Post-its .............................. every other field concatenated into
#                                           one blob per row.
#   Color ................................. UI hex code.
#   Grade Level ........................... 99.2% blank once non-high-school
#                                           rows are dropped, and 8 of the 53
#                                           survivors hold "41193", an Excel
#                                           serial date that leaked in.
#   Subject Area .......................... dropped by request. Had an 81-row
#                                           tail of abbreviation codes (ENG,
#                                           SCI, PE, SOC, WLA...) alongside the
#                                           canonical values, all of them Active.
#   School Level .......................... used as the row filter (High School
#                                           plus K-12), then dropped.
#   Teacher Certification Required ........ dropped by request.
#   Everything else ....................... dropped by request.

# CPS writes "not applicable" into text fields as the literal string "N/A".
# It is a null, not a value: without this, HOMEROOM lands with a graduation
# credit type called "N/A". Note this is NOT the same as "No credit", which is
# a real graduation-credit category on 394 courses and is left alone.
NULL_TOKENS = ["N/A"]

SCHOOL_LEVEL_COL = "School Level"

# "High School" is the catalog's own label for secondary courses, but the four
# K-12 rows -- Student Meal, Staff Meal, Teacher Preparation, Staff Duty -- are
# scheduled here too. Student Meal (000011N) alone sits on every student's
# schedule, so dropping K-12 would lose those enrolments on any join.
SCHOOL_LEVEL_KEEP = ["High School", "K-12"]


def readExport(source_folder):
    """
    Reads the CPS Course Catalog export.

    A normal quoted CSV -- not a paginated print report -- so it needs no
    per-page header handling. It does need csv.reader rather than line
    splitting: 781 Description cells and 183 graduation-credit cells contain
    literal newlines inside their quotes, so a 7,572-row file spans 17,628
    physical lines.

    The source file is opened read-only.
    """
    csv_file = findExportFile(source_folder)
    csv_path = os.path.join(source_folder, csv_file)

    print(Fore.GREEN + f"Opening '{csv_file}'...")
    with open(csv_path, "r", newline="", encoding="utf-8-sig") as fh:
        rows = list(csv.reader(fh))

    if not rows:
        raise ValueError(f"'{csv_file}' is empty.")

    header = [h.strip() for h in rows[0]]
    missing = [h for h in list(COLUMN_MAPPING) + [SCHOOL_LEVEL_COL] if h not in header]
    if missing:
        raise ValueError(
            f"'{csv_file}' is missing expected column(s): {missing}. "
            "Expected a CPS Course Catalog export."
        )

    df = pd.DataFrame(rows[1:], columns=header)
    print(Fore.GREEN + f"Parsed {len(df)} catalog rows from '{csv_file}'.")
    return csv_file, df


def splitCreditTypes(raw):
    """
    Splits the comma-separated graduation credit list into positional columns.

    This field is the course's list of graduation requirements it can satisfy,
    and it mixes two kinds of entry: broad areas ("Elective", "Career Ed",
    "Fine Arts") and specific requirement slots ("English IV Content 1",
    "Adv Alg/Trig", "Non-Credit: Constitution Exam Graduation Requirement").
    87 distinct values appear across the file.

    THE ORDER IS A DESCENDING PRIORITY, NOT AN ARBITRARY LIST. A course fills
    the highest unmet requirement first and only ONE at a time: "RS1 Urban
    Studies" is [Other Social Science, Career Ed, Elective], so it satisfies
    Other Social Science if that is outstanding, otherwise Career Ed, otherwise
    Elective. Preserve the order on load -- it is the whole basis of working out
    what a student still needs.
    """
    parts = [p.strip() for p in str(raw).split(",") if p.strip()]
    if len(parts) > MAX_CREDIT_TYPES:
        raise ValueError(
            f"{len(parts)} credit types on one course, more than the "
            f"{MAX_CREDIT_TYPES} columns available: {parts}"
        )
    parts += [None] * (MAX_CREDIT_TYPES - len(parts))
    return parts


def cleanData(df):
    print("Cleaning data...")

    # Collapse the newlines that live inside quoted cells before anything else
    # reads them, or a value arrives as "Fine Arts,\n Career Ed".
    for c in df.columns:
        df[c] = df[c].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()

    before = len(df)
    df = df[df[SCHOOL_LEVEL_COL].isin(SCHOOL_LEVEL_KEEP)].copy()
    print(Fore.GREEN + f"  kept {len(df)} {'/'.join(SCHOOL_LEVEL_KEEP)} rows, "
                       f"dropped {before - len(df)}.")

    df = df[list(COLUMN_MAPPING)].rename(columns=COLUMN_MAPPING)

    # Blank these before anything else reads them, so the credit split sees an
    # empty string and yields ten nulls rather than a creditType1 of "N/A".
    df = df.replace(NULL_TOKENS, "")

    df["active"] = df["active"].str.lower().map({"yes": True, "no": False})

    # Every high-school row is numeric here. The "N/A" string that appears in
    # the raw export is an elementary-school artifact and does not survive the
    # filter above, so anything non-numeric now is a real problem worth seeing.
    df["creditsEarned"] = pd.to_numeric(df["creditsEarned"], errors="coerce")

    credits = df["gradCreditRaw"].apply(splitCreditTypes)
    for i, col in enumerate(CREDIT_COLS):
        df[col] = [c[i] for c in credits]

    df = df.replace({"": None})
    df = df[ORDERED_COLS]

    return df


def checkCourseCatalog(df):
    """
    Reports anything odd. Advisory only -- this export carries no totals row.
    """
    print(Fore.GREEN + "Checking the course catalog pull...")
    print(Fore.GREEN + f"  {len(df)} courses, {int(df['active'].sum())} active.")

    filled = int(df["creditType1"].notna().sum())
    total = int(sum(df[c].notna().sum() for c in CREDIT_COLS))
    print(Fore.GREEN + f"  {total} course/credit-type pairs across "
                       f"{filled} courses with at least one.")

    dupes = int(df["courseNumber"].duplicated().sum())
    if dupes:
        print(Fore.YELLOW + f"  {dupes} duplicate course number(s); this column "
                            "is the table's key and should be unique.")

    noCredit = int(df["creditType1"].isna().sum())
    if noCredit:
        print(Fore.YELLOW + f"  {noCredit} course(s) list no graduation credit type.")

    badCredits = int(df["creditsEarned"].isna().sum())
    if badCredits:
        print(Fore.YELLOW + f"  {badCredits} course(s) have a non-numeric "
                            "'Number of Credits Earned'.")

    noIsbe = int(df["isbeNumber"].isna().sum())
    if noIsbe:
        print(Fore.YELLOW + f"  {noIsbe} course(s) have no ISBE number "
                            "(mostly inactive courses); stored as null.")


def doWork():
    """
    Main function for the script.
    """
    print(Style.BRIGHT + f"Starting {scriptName} script...")

    # Read the export. Nothing is written back to it.
    csv_file, rawDataFrame = readExport(source_folder)

    # Clean the data
    cleanedDataFrame = cleanData(rawDataFrame)

    checkCourseCatalog(cleanedDataFrame)

    # Replace the table contents. The delete and the upload are ONE operation --
    # never enable one without the other, or a failed run leaves the table empty.
    # ENABLED 2026-08-28. Table created from tableSchema above on the same day.
    deleteAllDataFromTable(project_id, dataset_id, table_id)
    uploadToBigQuery(cleanedDataFrame, tableSchema, project_id, dataset_id, table_id)

    # Archive the source file
    # NOTE: archiveSourceFile writes the cleaned frame to archivedFiles/ and then
    # DELETES the source export. Keep a spare copy while testing.
    archiveSourceFile(cleanedDataFrame, csv_file, source_folder, table_id)

    print(Style.BRIGHT + f"{scriptName} data pull complete.")


if __name__ == "__main__":
    doWork()
