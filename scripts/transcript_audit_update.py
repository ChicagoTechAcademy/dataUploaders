import os
import re
import csv
import pandas as pd
from _dataManager import (
    findExportFile,
    fetchDataFromBigQuery,
    deleteAllDataFromTable,
    uploadToBigQuery,
    archiveSourceFile,
)

from colorama import init, Fore, Style
init(autoreset=True)


scriptName = "Transcript Audit"


# Constants
project_id = "chitechdb"
dataset_id = "academics"
table_id = "transcriptAudit"
source_folder = f"../dataUploaders/{table_id}"

CATALOG_TABLE = "academics.cpsCourseCatalog"
CHITECH = "CHICAGO TECH HS"

MAX_CREDIT_TYPES = 4
CREDIT_COLS = [f"creditType{i}" for i in range(1, MAX_CREDIT_TYPES + 1)]

# The catalog carries ten credit-type slots; only four are loaded here. Slot 5 is
# fetched but not stored, purely so an overflow can be reported instead of
# vanishing -- it was empty on all 7,524 rows of the 28 Aug 2026 pull, but four
# courses in the catalog do carry more than four types.
OVERFLOW_COL = f"creditType{MAX_CREDIT_TYPES + 1}"

tableSchema = [
    {"name": "name", "type": "STRING"},
    {"name": "id", "type": "INTEGER"},
    {"name": "yearTaken", "type": "INTEGER"},
    {"name": "gradeLevel", "type": "STRING"},
    {"name": "courseNumber", "type": "STRING"},
    {"name": "transcriptDescription", "type": "STRING"},
    {"name": "creditEarned", "type": "FLOAT"},
    {"name": "creditPossible", "type": "FLOAT"},
    {"name": "finalGrade", "type": "STRING"},
    {"name": "classCode", "type": "STRING"},
    {"name": "teacher", "type": "STRING"},
    {"name": "school", "type": "STRING"},
    {"name": "termTaken", "type": "STRING"},
    {"name": "semesterCredit", "type": "STRING"},
    {"name": "course", "type": "STRING"},
    {"name": "courseType", "type": "STRING"},
    {"name": "atChiTech", "type": "BOOLEAN"},
    {"name": "gradCreditRaw", "type": "STRING"},
] + [{"name": c, "type": "STRING"} for c in CREDIT_COLS]

ORDERED_COLS = [c["name"] for c in tableSchema]

# Source header -> warehouse column. Everything not named here is dropped.
COLUMN_MAPPING = {
    "Student > Name": "name",
    "Student > Student ID": "id",
    "SchoolYear > Year": "yearTaken",
    "Grade": "gradeLevel",
    "SchoolCourse > CrsNo": "courseNumber",
    "SchoolCourse > Description": "transcriptDescription",
    "Credit": "creditEarned",
    "SchoolCourse > Credit": "creditPossible",
    "FG": "finalGrade",
    "MasterSchedule > Class": "classCode",
    "MasterSchedule > Teacher": "teacher",
    "School > Name": "school",
    "MasterSchedule > Term": "_masterTerm",
    "Term": "_term",
}

# NOTE -- dropped columns, and why:
#   ExcGPA, RankExclude, DropCredit, PotentialCredit,
#   EquivalentSchoolCourse > Weight, SchoolCourse > Weight ... dropped by request.
#   Student > EnrStatus ..... "Enrolled" on every row; carries no information.
#   SchoolYear > YearName ... "School Year: 2024-2025", fully derivable from
#                             SchoolYear > Year, which is kept as yearTaken.
#
# "MasterSchedule > Term" and "Term" are merged into one column, termTaken --
# they are complementary, not duplicates. "Term" is populated on exactly the 540
# rows where "MasterSchedule > Term" is blank, and it is the only source that
# records summer school ("SS"). Where both are populated they agree.
#
# The catalog's own title is not stored. transcriptDescription keeps the text as
# it appeared on the transcript, and `course` carries the catalog's name with the
# code prefix stripped, so nothing is lost by dropping the third variant.

# The course-code prefix that opens a description: one level letter followed by
# a term marker, e.g. "RS1 Chemistry" or "HFY Biology". The level letter is
# optional -- one row reads "S1 English I" with no letter at all.
#
# Only this exact grammar is stripped. A bare two-letter opener such as "AP" or
# "DE" is part of the course name ("AP Research", "DE Physics 131") and must
# survive.
PREFIX_RE = re.compile(r"^([RAHNSDIP]?)(S[12]|F[YS]|SY|CR|Q[1-4])\s+")


def readExport(source_folder):
    """
    Reads the transcript audit export.

    A normal quoted CSV. Read with csv.reader rather than by line so that any
    quoted comma inside a course description or a teacher name stays intact.

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
            "Expected a transcript audit export."
        )

    # Trailing blank lines are ordinary in this export -- csv.reader returns
    # them as empty lists, which become rows of all-NA and break anything that
    # expects a string.
    data = [r for r in rows[1:] if r and any(c.strip() for c in r)]
    df = pd.DataFrame(data, columns=header)
    print(Fore.GREEN + f"Parsed {len(df)} transcript rows from '{csv_file}'.")
    return csv_file, df


def fetchCatalog():
    """
    Pulls the CPS course catalog to resolve each transcript row's course.

    Joined on course NUMBER only. Course descriptions on a transcript are typed
    in by hand from a paper transcript when a student transfers, so they are not
    authoritative -- and the same description legitimately appears against
    different course numbers, so matching on it would be wrong even if the
    typing were perfect.
    """
    print(Style.BRIGHT + Fore.BLUE + f"Fetching the course catalog from {CATALOG_TABLE}...")
    cols = ", ".join(["courseNumber", "title", "academicLevel", "gradCreditRaw"]
                     + CREDIT_COLS + [OVERFLOW_COL])
    cat = fetchDataFromBigQuery(f"SELECT {cols} FROM {CATALOG_TABLE}")
    print(Fore.BLUE + f"  {len(cat)} courses in the catalog.")
    return cat


def splitPrefix(description):
    """
    Returns (semesterMarker, courseNameWithoutPrefix) for one description.

    The prefix encodes which semester's credit the course satisfies -- "RS1" is
    first-semester Regular, "HS2" second-semester Honors. This is a property of
    the course, NOT of when the student sat it: a course taken in summer school
    still gives credit for the semester its code names. When the course was
    taken is `termRaw` / `masterTerm`, kept separately.

    Courses with no prefix are the non-credit `N` codes -- service learning
    projects, Health Education, competency tests. They satisfy no semester, so
    the marker is None rather than a guess.
    """
    # pd.isna, not `or`: a NaN is truthy, so `description or ""` passes the NaN
    # straight through and .strip() then fails on a float. Under pandas 3.0 an
    # astype(str) on an Arrow-backed column preserves NA rather than writing the
    # string "nan", so NA reaches here even after the frame has been stringified.
    if description is None or (isinstance(description, float) and pd.isna(description)) \
       or description is pd.NA:
        return None, ""
    text = str(description).strip()
    m = PREFIX_RE.match(text)
    if not m:
        return None, text
    return m.group(2), text[m.end():].strip()


def cleanData(df, cat):
    print("Cleaning data...")

    for c in df.columns:
        df[c] = df[c].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()

    before = len(df)
    df = df[list(COLUMN_MAPPING)].rename(columns=COLUMN_MAPPING)

    # Join on the course number. Left join -- a course missing from the catalog
    # keeps its row with the catalog fields null; see checkTranscriptAudit.
    df = df.merge(cat, on="courseNumber", how="left")
    if len(df) != before:
        raise ValueError(
            f"Catalog join changed the row count from {before} to {len(df)}. "
            "courseNumber must be unique in the catalog."
        )

    df = df.rename(columns={"academicLevel": "courseType"})

    # When the course was taken. "Term" fills exactly the rows "MasterSchedule >
    # Term" leaves blank, and is the only column that records summer school.
    # This is NOT semesterCredit -- a course sat in summer still earns credit for
    # the semester its course code names.
    df["termTaken"] = df["_masterTerm"].replace("", None).fillna(
        df["_term"].replace("", None))

    # Semester and course name come from the description prefix, falling back to
    # the catalog title. Verified on the 28 Aug 2026 pull: where both carry a
    # prefix they agree on all 6,966 rows, on both semester and level.
    fromDesc = df["transcriptDescription"].apply(splitPrefix)
    fromTitle = df["title"].fillna("").apply(splitPrefix)

    df["semesterCredit"] = [d[0] if d[0] else t[0] for d, t in zip(fromDesc, fromTitle)]

    # Prefer the catalog's name for the course; fall back to the transcript's own
    # text only when the course is not in the catalog at all.
    matched = df["title"].notna()
    df["course"] = [t[1] if ok else d[1]
                    for d, t, ok in zip(fromDesc, fromTitle, matched)]
    df["_matched"] = matched

    df["atChiTech"] = df["school"].eq(CHITECH)

    for c in ["id", "yearTaken"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    for c in ["creditEarned", "creditPossible"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.replace({"": None})

    # Keep the overflow flag off the frame that gets uploaded, but hand it to
    # the checks first.
    df.attrs["overflowRows"] = int(df[OVERFLOW_COL].notna().sum())
    df.attrs["unmatched"] = df.loc[~df["_matched"],
                                   ["courseNumber", "transcriptDescription"]].copy()
    df = df[ORDERED_COLS]

    return df


def checkTranscriptAudit(df, rawRowCount):
    """
    Reports anything odd. Advisory only -- this export carries no totals row.
    """
    print(Fore.GREEN + "Checking the transcript audit pull...")

    if len(df) != rawRowCount:
        print(Fore.YELLOW + f"  row count changed: {rawRowCount} in, {len(df)} out.")
    else:
        print(Fore.GREEN + f"  {len(df)} rows in, {len(df)} rows out.")

    print(Fore.GREEN + f"  {df['id'].nunique()} students, "
                       f"{df['courseNumber'].nunique()} distinct courses, "
                       f"{int(df['atChiTech'].sum())} rows at ChiTech.")

    unmatched = df.attrs.get("unmatched", df.iloc[0:0])
    if len(unmatched):
        print(Fore.YELLOW + f"  {len(unmatched)} row(s) name a course that is not in "
                            f"{CATALOG_TABLE}. Catalog fields are null; the row is kept:")
        for cn, grp in unmatched.groupby("courseNumber"):
            print(Fore.YELLOW + f"    {cn}  {grp['transcriptDescription'].iloc[0]}  "
                                f"({len(grp)} row(s))")

    noTerm = int(df["termTaken"].isna().sum())
    if noTerm:
        print(Fore.YELLOW + f"  {noTerm} row(s) have no termTaken; neither term "
                            "column was populated.")

    noSem = int(df["semesterCredit"].isna().sum())
    if noSem:
        print(Fore.YELLOW + f"  {noSem} row(s) have no semesterCredit. Expected -- these are "
                            "the non-credit 'N' courses (service learning, competency "
                            "tests) which satisfy no semester.")

    overEarned = int((df["creditEarned"] > df["creditPossible"]).sum())
    if overEarned:
        print(Fore.YELLOW + f"  {overEarned} row(s) earned more credit than the course "
                            "is worth.")

    overflow = df.attrs.get("overflowRows", 0)
    if overflow:
        print(Fore.YELLOW + f"  {overflow} row(s) name a course with more than "
                            f"{MAX_CREDIT_TYPES} graduation credit types. Only the "
                            f"first {MAX_CREDIT_TYPES} are stored; the rest are "
                            "dropped. Check gradCreditRaw for the full list.")

    noGrade = int(df["finalGrade"].isna().sum())
    if noGrade:
        print(Fore.YELLOW + f"  {noGrade} row(s) have no final grade.")


def doWork():
    """
    Main function for the script.
    """
    print(Style.BRIGHT + f"Starting {scriptName} script...")

    # Read the export. Nothing is written back to it.
    csv_file, rawDataFrame = readExport(source_folder)
    rawRowCount = len(rawDataFrame)

    cat = fetchCatalog()

    # Clean the data
    cleanedDataFrame = cleanData(rawDataFrame, cat)

    checkTranscriptAudit(cleanedDataFrame, rawRowCount)

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
