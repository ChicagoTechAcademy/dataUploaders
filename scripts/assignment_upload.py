import os
import csv
import pandas as pd
from _dataManager import (
    findExportFile,
    uploadToBigQuery,
    archiveSourceFile,
    getSchoolYear,
)

from colorama import init, Fore, Style
init(autoreset=True)


scriptName = "Assignment Breakdown by Letter Grade Report"


# Constants
project_id = "chitechdb"
dataset_id = "academics"
table_id = "assignments"
source_folder = f"../dataUploaders/{table_id}"

# Order matches academics.assignments exactly.
schema = [
    {"name": "entryID", "type": "INTEGER"},
    {"name": "term", "type": "STRING"},
    {"name": "teacher", "type": "STRING"},
    {"name": "class", "type": "STRING"},
    {"name": "period", "type": "STRING"},
    {"name": "assignment", "type": "STRING"},
    {"name": "num_A", "type": "INTEGER"},
    {"name": "num_B", "type": "INTEGER"},
    {"name": "num_C", "type": "INTEGER"},
    {"name": "num_D", "type": "INTEGER"},
    {"name": "num_F", "type": "INTEGER"},
    {"name": "num_blank", "type": "INTEGER"},
    {"name": "special_codes", "type": "INTEGER"},
    {"name": "num_grades", "type": "INTEGER"},
    {"name": "sy", "type": "STRING"},
]

ORDERED_COLS = [c["name"] for c in schema]

# A data row left-compacts to exactly these twelve values, in this order.
# The first four are text, the last eight are counts.
DATA_FIELDS = [
    "teacher",
    "class",
    "period",
    "assignment",
    "num_A",
    "num_B",
    "num_C",
    "num_D",
    "num_F",
    "num_blank",
    "special_codes",
    "num_grades",
]
COUNT_FIELDS = DATA_FIELDS[4:]

# ASPEN writes bare period numbers here but "01(Reg)" in the schedules export.
# Normalised so the two tables join.
PERIOD_MAPPING = {
    "01": "01(Reg)",
    "02": "02(Reg)",
    "03": "03(Reg)",
    "04": "04(Reg)",
    "05": "05(Reg)",
    "06": "06(Reg)",
    "07": "07(Reg)",
    "08": "08(Reg)",
    "Lu": "Lunch/Adv(Reg)",
    "Ad": "Adv/Lunch(Reg)",
}


# Get the entry ID from the user
def getEntryID():
    # entryID = input(Fore.CYAN + "Enter the entry ID for this roster YYYYMMDDX: ")

    # find todays date, and reformat it as YYYYMMDD,
    entryID = pd.Timestamp.now().strftime("%Y%m%d")

    print(Fore.CYAN + f"The EntryID is {entryID}.")

    return int(entryID)


def getTerm():
    """
    Asks for the term label. Any value is accepted, as typed.

    Term is NOT derived. The digit in a class prefix ("RS1", "RS2") is the
    semester, which is a different quantity -- historical rows here hold Q1-Q4,
    and a couple of pulls used other labels. The label is yours to choose, so
    the only thing enforced is that it is not blank.

    Earlier pulls stopped recording it and wrote "-"; those 26,886 rows were
    backfilled to Q4 on 2026-08-28.
    """
    while True:
        term = input(Fore.CYAN + "Enter the term/label for this pull (Q1, S1, EndOfQ1, ...): ").strip()
        if term:
            return term
        print(Fore.YELLOW + "  Term cannot be blank.")


def compact(row):
    """
    Drops empty cells and returns the remaining values, in order.

    This export's column positions drift: most rows carry 13 fields, some carry
    14, and everything from Period onward shifts by one between the two shapes.
    Left-compacting collapses both to the same twelve values, which is why the
    positions below are stable where raw indices are not.
    """
    return [c.strip() for c in row if c.strip() != ""]


def isDataRow(values):
    """
    A data row compacts to exactly twelve values whose last eight are numeric.

    Subtotal rows, the title banner and the two-line header all fail this and
    drop out on their own -- no row deletion by position, and no need to strip
    the first line off the file.
    """
    if len(values) != len(DATA_FIELDS):
        return False
    return all(v.replace(".", "", 1).lstrip("-").isdigit() for v in values[4:])


def isTotalsRow(values):
    return bool(values) and values[0].rstrip(": ").lower() == "totals"


def readExport(source_folder):
    """
    Reads the assignments export and classifies every row.

    Read with csv.reader, not line splitting: the header's "special codes" cell
    contains a literal newline inside its quotes, so the header spans two
    physical lines but is a single record.

    The source file is opened read-only. The previous version rewrote it in
    place to strip the first line, which was not idempotent -- a second run ate
    the header row too.

    Returns (filename, DataFrame, list of subtotal rows).
    """
    csv_file = findExportFile(source_folder)
    csv_path = os.path.join(source_folder, csv_file)

    print(Fore.GREEN + f"Opening '{csv_file}'...")
    with open(csv_path, "r", newline="", encoding="utf-8-sig") as fh:
        rows = list(csv.reader(fh))

    records, totals, unknown = [], [], []
    for i, raw in enumerate(rows):
        values = compact(raw)
        if not values:
            continue
        if isTotalsRow(values):
            # Boundary = how many data rows have been collected so far, so each
            # subtotal knows exactly which slice of the frame it summarises.
            totals.append({"endsAt": len(records), "stated": values[1:]})
        elif isDataRow(values):
            records.append(dict(zip(DATA_FIELDS, values)))
        else:
            unknown.append((i, values))

    # The banner and the header are expected to land here; anything else is not.
    if len(unknown) > 2:
        print(Fore.YELLOW + f"{len(unknown)} row(s) matched neither shape:")
        for i, v in unknown[:5]:
            print(Fore.YELLOW + f"    row {i}: {v[:6]}")

    if not records:
        raise ValueError(
            f"Parsed 0 assignment rows from '{csv_file}'. Expected an "
            "Assignment Breakdown by Letter Grade export."
        )

    df = pd.DataFrame(records)
    print(Fore.GREEN + f"Parsed {len(df)} assignment rows from '{csv_file}'.")
    return csv_file, df, totals


def reconcileTotals(df, totals):
    """
    Checks each subtotal row against the data rows it summarises.

    The export prints a "Totals:" line per teacher, and those lines are a real
    checksum: on the 28 Aug 2026 pull all five reconciled exactly. Returns True
    when every group matches.
    """
    if not totals:
        print(Fore.YELLOW + "No 'Totals:' rows found; cannot reconcile.")
        return False

    print(Fore.GREEN + "Reconciling against the report's own 'Totals:' rows...")
    ok = True
    lo = 0
    for n, t in enumerate(totals, start=1):
        hi, stated = t["endsAt"], t["stated"]
        grp = df.iloc[lo:hi]
        lo = hi
        if len(stated) != len(COUNT_FIELDS) or grp.empty:
            print(Fore.YELLOW + f"  group {n}: unexpected shape "
                                f"({len(grp)} rows, {len(stated)} totals)")
            ok = False
            continue
        sums = [float(grp[c].sum()) for c in COUNT_FIELDS]
        matched = all(abs(a - float(b)) < 0.01 for a, b in zip(sums, stated))
        ok &= matched
        print((Fore.GREEN if matched else Fore.YELLOW)
              + f"  group {n}  {len(grp):>3} rows  "
              + f"parsed={[int(x) for x in sums]}  "
              + f"report={[int(float(b)) for b in stated]}"
              + ("  MATCH" if matched else "  MISMATCH"))

    if lo != len(df):
        print(Fore.YELLOW + f"  {len(df) - lo} data row(s) fall after the last "
                            "'Totals:' line and were not covered by any subtotal.")
        ok = False

    if not ok:
        print(Fore.YELLOW + "Reconciliation FAILED: parsed counts do not match the "
                            "report's own subtotals.")
    return ok


def cleanData(df, totals):
    entryID = getEntryID()

    print("Cleaning data...")

    df = df.copy()
    df["period"] = df["period"].map(PERIOD_MAPPING).fillna(df["period"])

    for c in COUNT_FIELDS:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("int64")

    df["teacher"] = df["teacher"].astype(str)
    df["class"] = df["class"].astype(str)
    df["assignment"] = df["assignment"].astype(str)
    df["period"] = df["period"].astype(str)

    df["entryID"] = int(entryID)
    df["term"] = getTerm()
    # Derive the school year rather than hardcoding it; see _dataManager.getSchoolYear.
    # Every row loaded before 2026-08-28 was backfilled to SY26.
    df["sy"] = getSchoolYear(pd.Timestamp.now().strftime("%Y-%m-%d"))

    df = df[ORDERED_COLS]

    return df


def doWork():
    """
    Main function for the script.
    """
    print(Style.BRIGHT + f"Starting {scriptName} script...")

    # Read the export. Nothing is written back to it.
    csv_file, rawDataFrame, totals = readExport(source_folder)

    # Clean the data
    cleanedDataFrame = cleanData(rawDataFrame, totals)

    # Verify the parse against the report's own subtotals before writing.
    if not reconcileTotals(cleanedDataFrame, totals):
        print(Fore.YELLOW + "Aborting: nothing uploaded, source file left in place.")
        raise SystemExit(1)

    # Upload the data to BigQuery
    # DISABLED -- enable when you decide to pull.
    uploadToBigQuery(cleanedDataFrame, schema, project_id, dataset_id, table_id)

    # Archive the source file
    # NOTE: archiveSourceFile writes the cleaned frame to archivedFiles/ and then
    # DELETES the source export. Keep a spare copy while testing.
    archiveSourceFile(cleanedDataFrame, csv_file, source_folder, table_id)

    print(Style.BRIGHT + f"{scriptName} data pull complete.")


if __name__ == "__main__":
    doWork()
