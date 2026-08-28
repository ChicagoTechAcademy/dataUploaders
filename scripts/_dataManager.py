from datetime import datetime
import re
import csv
import pandas as pd
import pandas_gbq
import google.auth
import os
from colorama import init, Fore, Style
init(autoreset=True)


_credentials = None


def getCredentials():
    """
    Returns the cached credentials, authenticating on first use.

    Reads the service account named by GOOGLE_APPLICATION_CREDENTIALS.
    BigQuery scope only -- no Drive, no cloud-platform.
    """
    global _credentials
    if _credentials is None:
        _credentials, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/bigquery"]
        )
    return _credentials

def fetchDataFromBigQuery(query):
    """
    Fetches data from BigQuery and returns a DataFrame.
    """
    print(Style.BRIGHT + Fore.BLUE + "Fetching data from BigQuery...")
    df = pandas_gbq.read_gbq(
        query,
        project_id="chitechdb",
        credentials=getCredentials(),
    )
    return df

def deleteAllDataFromTable(project_id, dataset_id, table_id):
    """
    Deletes all data from the specified BigQuery table.
    """
    print(Style.BRIGHT + Fore.BLUE + "Deleting old data from database to override...")
    pandas_gbq.context.credentials = getCredentials()
    pandas_gbq.context.project = project_id
    query = f"DELETE FROM `{dataset_id}.{table_id}` WHERE TRUE"
    pandas_gbq.read_gbq(query)
    print(Style.BRIGHT + Fore.BLUE + f"All data from {table_id} has been deleted.")

def deleteDataBetweenDates(project_id, dataset_id, table_id, min_date, max_date):
    """
    Deletes data from the specified BigQuery table within a specified date range.
    """
    print(Style.BRIGHT + Fore.BLUE + "Deleting data from database within date range...")
    pandas_gbq.context.credentials = getCredentials()
    pandas_gbq.context.project = project_id
    query = f"DELETE FROM `{dataset_id}.{table_id}` WHERE date BETWEEN '{min_date}' AND '{max_date}'"
    pandas_gbq.read_gbq(query)
    print(Style.BRIGHT + Fore.BLUE + "Data deletion completed.")


def uploadToBigQuery(df, schema, project_id, dataset_id, table_id):
    """
    Uploads a pandas DataFrame to a BigQuery table.
    """

    print(Style.BRIGHT + Fore.BLUE + "Uploading data to BigQuery...")
    # Upload the dataframe
    pandas_gbq.to_gbq(
        df,
        destination_table=dataset_id + "." + table_id,
        project_id=project_id,
        if_exists="append",
        progress_bar=True,
        table_schema=schema,
    )
    print(Style.BRIGHT + Fore.BLUE + "Data has been uploaded to BigQuery.")


def archiveSourceFile(df, sourceFile, sourceFolder, archiveFileName):
    """
    Moves the source file to an archive directory.
    """
    destination_folder = "../dataUploaders/archivedFiles"

    current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    new_file_name = f"{archiveFileName}-{current_datetime}.csv"
    destination_file_path = os.path.join(destination_folder, new_file_name)
    df.to_csv(destination_file_path, index=False)
    os.remove(os.path.join(sourceFolder, sourceFile))
    print(Fore.GREEN + f"File '{sourceFile}' has been archived as '{new_file_name}'.")


# ---------------------------------------------------------------------------
# Paginated print-report parsing
#
# Several ASPEN exports are print reports dumped to CSV rather than data
# exports. They are paginated, and the column layout changes from page to page
# -- a single Average Daily Attendance file can carry three different layouts.
# Each page states its own layout in a two-row header, so fields are read by
# name from that page's header instead of from hardcoded positions. Nothing
# ever needs shifting or realigning, and the source file is never modified.
# ---------------------------------------------------------------------------

def findExportFile(source_folder):
    """
    Returns the single CSV in the source folder.

    Exactly one candidate is required. Zero or several is an error rather than
    a coin flip, and Excel lock files (~$...) are ignored.
    """
    candidates = [
        f for f in os.listdir(source_folder)
        if f.lower().endswith(".csv") and not f.startswith("~$")
    ]
    if not candidates:
        raise FileNotFoundError(f"No .csv file found in '{source_folder}'.")
    if len(candidates) > 1:
        raise ValueError(
            f"Expected exactly one .csv in '{source_folder}', found {len(candidates)}: "
            + ", ".join(sorted(candidates))
        )
    return candidates[0]


def buildFieldIndex(upperRow, headerRow):
    """
    Builds {field name -> column index} for one page.

    ASPEN splits a header across two rows: "Days" sits above "Enrolled". Joining
    the two rows per column index reconstructs the real field name. Reports with
    a single-row header (grades) simply have a blank row above, which
    contributes nothing.

    Trailing colons are stripped, so "Student ID:" and "Student ID" are the same
    field. Match on normalized header text, never on an exact string.
    """
    fields = {}
    for j in range(max(len(upperRow), len(headerRow))):
        upper = upperRow[j].strip().rstrip(":") if j < len(upperRow) else ""
        lower = headerRow[j].strip().rstrip(":") if j < len(headerRow) else ""
        name = (upper + " " + lower).strip()
        if name:
            fields[name] = j
    return fields


def parsePaginatedReport(rows, fieldMap):
    """
    Extracts data rows from a paginated ASPEN print report.

    fieldMap maps the report's own header text to output column names, e.g.
    {"ID": "id", "Days Enrolled": "daysEnrolled"}.

    Grade level comes from the page banner ("Grade Level: 09") rather than from
    a column, and is carried down to that page's rows.
    """
    records = []
    fields = None
    gradeLevel = None

    for i, row in enumerate(rows):
        m = re.search(r"Grade Level:\s*(\w+)", ",".join(row))
        if m:
            gradeLevel = m.group(1)
            continue

        # A row is this page's header when combining it with the row above
        # yields every field the caller asked for. This works whether the header
        # occupies one row or two, and it re-reads on every page, which is what
        # makes changing layouts a non-issue.
        candidate = buildFieldIndex(rows[i - 1] if i > 0 else [], row)
        if all(name in candidate for name in fieldMap):
            fields = candidate
            continue

        # A data row starts with a numeric ID. Subtotals, symbol keys, grand
        # totals and page furniture all fail this test and drop out.
        if row and row[0].strip().isdigit() and fields:
            record = {"gradeLevel": gradeLevel}
            for headerName, colName in fieldMap.items():
                j = fields.get(headerName)
                record[colName] = row[j].strip() if j is not None and j < len(row) else ""
            records.append(record)

    return records


def parseReportTotals(rows, totalsMap, valueCol=12):
    """
    Reads the report's own GRAND TOTALS block.

    These exports ship with their own checksums. Using them turns "the script
    did not crash" into "the parsed numbers equal what ASPEN says they are."

    totalsMap maps the label used in the totals block to an output column name;
    the totals block sometimes spells a field differently from its column
    header. Returns {column name -> total} plus "studentCount".
    """
    start = None
    for i, row in enumerate(rows):
        if any(c.strip() == "GRAND TOTALS" for c in row):
            start = i
            break
    if start is None:
        return {}

    totals = {}
    for i in range(start, len(rows) - 1):
        label = rows[i][3].strip() if len(rows[i]) > 3 else ""
        if label not in totalsMap and label != "Appearing On Report":
            continue
        valueRow = rows[i + 1]
        if len(valueRow) <= valueCol:
            continue
        try:
            value = float(valueRow[valueCol].strip())
        except ValueError:
            continue
        key = "studentCount" if label == "Appearing On Report" else totalsMap[label]
        totals.setdefault(key, value)

    return totals


def parseTotalRow(rows):
    """
    Reads a trailing "Total" row, where one exists.

    Returns the last numeric value on that row, or None. What the number counts
    is report-specific -- read it, report it, and do not assume it is a row
    count.
    """
    for row in rows:
        if any(c.strip() == "Total" for c in row):
            for cell in reversed(row):
                try:
                    return float(cell.strip())
                except ValueError:
                    continue
    return None


def reconcile(df, totals, columns):
    """
    Compares the parsed frame against the report's own stated totals.

    Prints one line per field. Returns True when everything matches.
    """
    if not totals:
        print(Fore.YELLOW + "No GRAND TOTALS block found; skipping reconciliation.")
        return False

    print(Fore.GREEN + "Reconciling against the report's own GRAND TOTALS...")
    ok = True

    def report(label, parsed, stated):
        matched = abs(parsed - stated) < 0.01
        line = f"  {label:<20} parsed={parsed:>9.2f}  report={stated:>9.2f}"
        print((Fore.GREEN if matched else Fore.YELLOW) + line
              + ("  MATCH" if matched else "  MISMATCH"))
        return matched

    if "studentCount" in totals:
        ok &= report("student count", float(len(df)), totals["studentCount"])
    for col in columns:
        if col in totals:
            ok &= report(col, float(df[col].sum()), totals[col])

    if not ok:
        print(Fore.YELLOW + "Reconciliation FAILED: parsed numbers do not match the report's own totals.")
    return ok


def readPaginatedExport(source_folder, fieldMap, totalsMap):
    """
    Reads and parses a paginated export.

    The source file is opened read-only and never written back; all repair
    happens on the parsed rows in memory.

    Returns (filename, DataFrame, totals).
    """
    csv_file = findExportFile(source_folder)
    csv_path = os.path.join(source_folder, csv_file)

    print(Fore.GREEN + f"Opening '{csv_file}'...")
    with open(csv_path, "r", newline="", encoding="utf-8") as infile:
        rows = list(csv.reader(infile))

    records = parsePaginatedReport(rows, fieldMap)
    totals = parseReportTotals(rows, totalsMap)

    if not records:
        raise ValueError(
            f"Parsed 0 data rows from '{csv_file}'. Expected a paginated ASPEN "
            "report with 'ID' header rows."
        )

    df = pd.DataFrame(records)
    print(Fore.GREEN + f"Parsed {len(df)} rows from '{csv_file}'.")
    return csv_file, df, totals


def readCSV(source_folder):
    """
    Reads a CSV file and returns the filename found and DataFrame from the read information
    """
    print(Fore.GREEN + "Opening the CSV file...")

    csv_files = [f for f in os.listdir(source_folder) if f.endswith(".csv")]

    if csv_files:
        csv_file = csv_files[0]
        csv_path = os.path.join(source_folder, csv_file)

        df = pd.read_csv(csv_path).dropna(how="all")
        print(
            Fore.GREEN
            + f"File '{csv_file}' has been read. Empty rows have been removed."
        )
        return csv_file, df

def convertToStandardDate(date_str):
    # if date_str is null, return null
    if pd.isnull(date_str):
        return None

    month, day, year = map(int, date_str.split("/"))
    if year < 100:
        if year >= 50:
            year += 1900
        else:
            year += 2000
    return f"{year:04d}-{month:02d}-{day:02d}"

def convertToStandardTime(time_str):
    # if time_str is null, return null
    if pd.isnull(time_str):
        return None

    hour, minute, second = map(int, time_str.split(":"))
    return f"{hour:02d}:{minute:02d}:{second:02d}"

def getSchoolYear(date):
    year = int(date.split('-')[0]) % 100
    month = int(date.split('-')[1])
    if month > 7:
        return f"SY{year+1}"
    else:
        return f"SY{year}"


def getSemester(date):
    month = int(date.split('-')[1])
    if month > 7:
        return "S1"
    else:
        return "S2"
    
