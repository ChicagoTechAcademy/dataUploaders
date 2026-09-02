import os
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


scriptName = "CPS High School Graduation Requirements"


# Constants
project_id = "chitechdb"
dataset_id = "academics"
summary_table = "gradReqSummary"
progress_table = "gradReqProgress"
source_folder = "../dataUploaders/cpsGradReqExport"

# One row per student.
summarySchema = [
    {"name": "entryID", "type": "INTEGER"},
    {"name": "id", "type": "INTEGER"},
    {"name": "name", "type": "STRING"},
    {"name": "gradeLevel", "type": "STRING"},
    {"name": "iep", "type": "STRING"},
    {"name": "programOfStudy", "type": "STRING"},
    {"name": "yearEntered9thGrade", "type": "INTEGER"},
    {"name": "yog", "type": "INTEGER"},
    {"name": "promotionStatus", "type": "STRING"},
    {"name": "graduationRequirementsMet", "type": "STRING"},
    {"name": "graduationNotesPresent", "type": "STRING"},
    {"name": "totalRequiredCredits", "type": "FLOAT"},
    {"name": "totalCreditsWaived", "type": "FLOAT"},
    {"name": "totalRequirementCreditsEarned", "type": "FLOAT"},
    {"name": "totalCreditsEarned", "type": "FLOAT"},
]

# One row per student per requirement.
progressSchema = [
    {"name": "entryID", "type": "INTEGER"},
    {"name": "id", "type": "INTEGER"},
    {"name": "requirement", "type": "STRING"},
    {"name": "requirementKind", "type": "STRING"},
    {"name": "catalogCreditType", "type": "STRING"},
    {"name": "status", "type": "STRING"},
    {"name": "creditsRequired", "type": "FLOAT"},
    {"name": "creditsEarned", "type": "FLOAT"},
]

SUMMARY_COLS = [c["name"] for c in summarySchema]
PROGRESS_COLS = [c["name"] for c in progressSchema]

STUDENT_MAPPING = {
    "Student ID": "id",
    "Student Name": "name",
    "Grade Level": "gradeLevel",
    "IEP": "iep",
    "Program of Study": "programOfStudy",
    "Year Entered 9th Grade": "yearEntered9thGrade",
    "Year of Graduation": "yog",
    "Promotion Status": "promotionStatus",
    "Graduation Requirements Met": "graduationRequirementsMet",
    "Graduation Notes Present": "graduationNotesPresent",
    "Total Required Credits": "totalRequiredCredits",
    "Total Credits Waived": "totalCreditsWaived",
    "Total Requirement Credits Earned": "totalRequirementCreditsEarned",
    "Total Credits Earned": "totalCreditsEarned",
}

# Requirements with no credit attached -- a task completed rather than credits
# earned. Their value is the status itself (Yes / No / N/A / Waived).
NON_CREDIT_REQUIREMENTS = [
    "Drivers Ed",
    "Constitution Exam",
    "Consumer/Financial Education",
    "Health Education",
    "State Mandated Exam",
    "Civics Service Learning Project",
    "Service Learning Project 2",
    "Service Learning Hours",
    "Learn.Plan.Succeed.",
    "Financial Aid Application",
]

# CPS's requirement name -> the matching value in cpsCourseCatalog.creditType*.
# 16 of the 21 credit requirements match verbatim and are resolved by name;
# only the mismatches are listed here.
#
# "Science" has no catalog equivalent -- the catalog names individual sciences
# (Biology, Chemistry, Physics, Environmental Science) while this export tracks
# a single 3-credit Science total. Left null rather than mapped to a guess.
CATALOG_CREDIT_TYPE = {
    "Other Lab Science": "Additional Lab Science",
    "World Lang I & II": "World Language",
    "Physical Ed I": "Physical Education I Content 1",
    "Physical Ed II": "Physical Education II Content 1",
    "Science": None,
}

# NOTE -- dropped columns, and why:
#   Network, School Name, School ID, Mapping Type ... dropped by request; all
#                                                     four are single-valued.
#   "Promotion" as a requirement .................... not one. "Promotion
#       Status" is a student-level field that a naive scan for columns ending in
#       " Status" picks up as if it were a requirement. It has no Required or
#       Earned column because it is not a credit. Kept as promotionStatus on the
#       summary row.
#
# NOTE -- two limits of this export that cannot be worked around here:
#
#   1. "Biology" and "Other Lab Science" are N/A for every student. Only
#      "Science" is live, as a single 3-credit total. The real rule is that
#      those 3 must be Biology, Chemistry and Physics -- WHICH sciences a
#      student has is not in this file. That needs academics.transcriptAudit.
#
#   2. One credit is two semesters. A student showing 0.5 of 1 has completed one
#      semester, and this export does not say WHICH. That also needs the
#      transcript audit.


def getEntryID():
    entryID = pd.Timestamp.now().strftime("%Y%m%d")
    print(Fore.CYAN + f"The EntryID is {entryID}.")
    return int(entryID)


def readExport(source_folder):
    """
    Reads the CPS High School Graduation Requirements export.

    A wide, one-row-per-student file: each requirement occupies three columns
    ("<name> Status", "<name> Required", "<name> Earned").

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
    missing = [h for h in STUDENT_MAPPING if h not in header]
    if missing:
        raise ValueError(
            f"'{csv_file}' is missing expected column(s): {missing}. Expected a "
            "CPS High School Graduation Requirements export."
        )

    df = pd.DataFrame(rows[1:], columns=header)
    df = df[df[list(STUDENT_MAPPING)[0]].str.strip() != ""]
    print(Fore.GREEN + f"Parsed {len(df)} student rows from '{csv_file}'.")
    return csv_file, df


def creditRequirements(header):
    """
    Finds the credit requirements by looking for the Required/Earned pair.

    A requirement is only a credit requirement if it has all three of Status,
    Required and Earned. Keying off " Status" alone would sweep up "Promotion
    Status", which is a student-level field.
    """
    names = []
    for col in header:
        if not col.endswith(" Status"):
            continue
        base = col[:-len(" Status")].strip()
        if f"{base} Required" in header and f"{base} Earned" in header:
            names.append(base)
    return names


def toFloat(series):
    """CPS writes 'N/A' where a requirement does not apply. That is a null."""
    return pd.to_numeric(series.replace({"N/A": None, "": None}), errors="coerce")


def buildSummary(df, entryID):
    out = df[list(STUDENT_MAPPING)].rename(columns=STUDENT_MAPPING).copy()
    for c in ["id", "yearEntered9thGrade", "yog"]:
        out[c] = pd.to_numeric(out[c], errors="coerce").astype("Int64")
    for c in ["totalRequiredCredits", "totalCreditsWaived",
              "totalRequirementCreditsEarned", "totalCreditsEarned"]:
        out[c] = toFloat(out[c])
    out["entryID"] = int(entryID)
    return out.replace({"": None})[SUMMARY_COLS]


def buildProgress(df, header, entryID):
    """
    Unpivots the wide export to one row per student per requirement.

    Credit requirements contribute status + required + earned. Non-credit
    requirements contribute status only, with both credit figures null -- they
    are tasks, not credits, and writing 0 would read as "earned nothing" rather
    than "does not apply".
    """
    ids = pd.to_numeric(df["Student ID"], errors="coerce").astype("Int64")
    frames = []

    for name in creditRequirements(header):
        frames.append(pd.DataFrame({
            "id": ids,
            "requirement": name,
            "requirementKind": "credit",
            "catalogCreditType": CATALOG_CREDIT_TYPE.get(name, name),
            "status": df[f"{name} Status"].replace({"": None}),
            "creditsRequired": toFloat(df[f"{name} Required"]),
            "creditsEarned": toFloat(df[f"{name} Earned"]),
        }))

    for name in NON_CREDIT_REQUIREMENTS:
        if name not in header:
            print(Fore.YELLOW + f"  non-credit requirement '{name}' is not in this "
                                "export; skipped.")
            continue
        frames.append(pd.DataFrame({
            "id": ids,
            "requirement": name,
            "requirementKind": "nonCredit",
            "catalogCreditType": None,
            "status": df[name].replace({"": None}),
            "creditsRequired": None,
            "creditsEarned": None,
        }))

    out = pd.concat(frames, ignore_index=True)
    out["entryID"] = int(entryID)
    return out[PROGRESS_COLS]


def cleanData(df, header):
    entryID = getEntryID()

    print("Cleaning data...")
    for c in df.columns:
        df[c] = df[c].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()

    summary = buildSummary(df, entryID)
    progress = buildProgress(df, header, entryID)
    return summary, progress


def checkGradRequirements(summary, progress):
    """
    Reports anything odd. Advisory only -- this export carries no totals row.
    """
    print(Fore.GREEN + "Checking the graduation requirements pull...")
    print(Fore.GREEN + f"  {len(summary)} students, {len(progress)} requirement rows "
                       f"({progress['requirement'].nunique()} distinct requirements).")

    perStudent = len(progress) / max(len(summary), 1)
    print(Fore.GREEN + f"  {perStudent:.0f} requirements per student.")

    dupes = int(summary["id"].duplicated().sum())
    if dupes:
        print(Fore.YELLOW + f"  {dupes} duplicate student id(s) in the summary.")

    unmapped = sorted(progress.loc[
        (progress["requirementKind"] == "credit") & progress["catalogCreditType"].isna(),
        "requirement"].unique())
    if unmapped:
        print(Fore.YELLOW + f"  {len(unmapped)} credit requirement(s) have no "
                            f"cpsCourseCatalog equivalent: {unmapped}")

    allNA = (progress[progress["requirementKind"] == "credit"]
             .groupby("requirement")["status"]
             .apply(lambda s: (s == "N/A").all()))
    dead = sorted(allNA[allNA].index)
    if dead:
        print(Fore.YELLOW + f"  {len(dead)} requirement(s) are N/A for EVERY student, "
                            f"so CPS is not tracking them here: {dead}")

    # Verified against the roster rather than assumed: the export can carry
    # students the roster has not caught up with.
    try:
        roster = fetchDataFromBigQuery(
            'SELECT DISTINCT id FROM studentInfo.roster WHERE sy = "SY27" '
            'AND entryID = (SELECT MAX(entryID) FROM studentInfo.roster WHERE sy = "SY27")')
        known = set(roster["id"].astype("Int64"))
        offRoster = summary[~summary["id"].isin(known)]
        if len(offRoster):
            print(Fore.YELLOW + f"  {len(offRoster)} student(s) are NOT on the current "
                                "SY27 roster. Loaded anyway:")
            for _, r in offRoster.iterrows():
                print(Fore.YELLOW + f"    {r['id']}  {r['name']}  grade {r['gradeLevel']}"
                                    f"  YOG {r['yog']}")
    except Exception as exc:
        print(Fore.YELLOW + f"  could not check against the roster ({type(exc).__name__}).")


def doWork():
    """
    Main function for the script.
    """
    print(Style.BRIGHT + f"Starting {scriptName} script...")

    # Read the export. Nothing is written back to it.
    csv_file, rawDataFrame = readExport(source_folder)
    header = list(rawDataFrame.columns)

    # Clean the data
    summary, progress = cleanData(rawDataFrame, header)

    checkGradRequirements(summary, progress)

    # Replace both tables. The deletes and the uploads are ONE operation --
    # never enable one without the others, or a failed run leaves the pair
    # inconsistent with each other.
    # ENABLED 2026-09-02. Both tables created the same day.
    deleteAllDataFromTable(project_id, dataset_id, summary_table)
    uploadToBigQuery(summary, summarySchema, project_id, dataset_id, summary_table)
    deleteAllDataFromTable(project_id, dataset_id, progress_table)
    uploadToBigQuery(progress, progressSchema, project_id, dataset_id, progress_table)

    # Archive the source file
    # NOTE: archiveSourceFile writes the frame to archivedFiles/ and then DELETES
    # the source export.
    archiveSourceFile(summary, csv_file, source_folder, summary_table)

    print(Style.BRIGHT + f"{scriptName} data pull complete.")


if __name__ == "__main__":
    doWork()
