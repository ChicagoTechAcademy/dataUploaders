import pandas as pd
import os
import csv
from _dataManager import (
    readCSV,
    uploadToBigQuery,
    deleteAllDataFromTable,
    archiveSourceFile,
    convertToStandardDate,
)

from colorama import init, Fore
init(autoreset=True)


scriptName = "Total Avg Daily Attendance Update"


# Constants
project_id = "chitechdb"
dataset_id = "attendance"
table_id = "totalAttendance"
source_folder = f"../dataUploaders/{table_id}"

tableSchema = [
    {"name": "entryID", "type": "INTEGER"},
    {"name": "week", "type": "STRING"},
    {"name": "id", "type": "INTEGER"},
    {"name": "name", "type": "STRING"},
    {"name": "daysEnrolled", "type": "FLOAT"},
    {"name": "daysNotEnrolled", "type": "FLOAT"},
    {"name": "daysPresent", "type": "FLOAT"},
    {"name": "daysExcused", "type": "FLOAT"},
    {"name": "daysNotExcused", "type": "FLOAT"},
]


# Define the column name mappings
column_mappings = {
    "entryID": "entryID",
    "week": "week",
    "id": "id",
    "name": "name",
    "daysEnrolled": "daysEnrolled",
    "daysNotEnrolled": "daysNotEnrolled",
    "daysPresent": "daysPresent",
    "daysExcused": "daysExcused",
    "daysNotExcused": "daysNotExcused",
}

# Get the entry ID from the user
def getEntryID():
    # entryID = input(Fore.CYAN + "Enter the entry ID for this roster YYYYMMDDX: ")

    # find todays date, and reformat it as YYYYMMDD,
    entryID = pd.Timestamp.now().strftime("%Y%m%d")

    print(Fore.CYAN + f"The EntryID is {entryID}.")

    return int(entryID)

def getWeek():
    return input(Fore.CYAN + "Enter the week for this roster (W01, W02, ...): ")


def prepareCSVFile(source_folder):
    """
    Open each CSV in the source folder (without using pandas),
    remove empty cells from each row, and rewrite the file in place.

    Notes:
    - An "empty cell" is any field that, after stripping whitespace, is an empty string.
    - Rows that become empty after removing empty cells are skipped.
    - This keeps the remaining cell order and left-compacts non-empty values.
    """
    csv_files = [f for f in os.listdir(source_folder) if f.lower().endswith('.csv')]

    if not csv_files:
        print(Fore.YELLOW + f"No CSV files found in '{source_folder}'.")
        return

    for csv_file in csv_files:
        csv_path = os.path.join(source_folder, csv_file)

        # Read all rows
        raw_rows = []
        with open(csv_path, 'r', newline='', encoding='utf-8') as infile:
            reader = csv.reader(infile)
            for row in reader:
                raw_rows.append(row)

        
        # Look through indexed column 6 to find rows that start with "Name". When you find one, save a reference to this row number called "tempRow". In that row number, look at the next rows until you find one that is blank. Shift all of those rows into indexed column 4. 

        # Then look at the row number that is saved in "tempRow". Look at the columns in that row until you find the first column that has "Enrolled". Look at the next rows in that column until you find one that is blank. Shift all of those rows into indexed column 10.

        # Then look at the row number that is saved in "tempRow", look at the next columns in that row until you find the second column that has "Enrolled". Look at the next rows in that column until you find one that is blank. Shift all of those rows into indexed column 12.

        # Then look at the row number that is saved in "tempRow", look at the columns in that row until you find a column that has "Present". Look at the next rows in that column until you find one that is blank. Shift all of those rows into indexed column 13.

        # Then look at the row number that is saved in "tempRow", look at the next columns in that row until you find the first column that has "Excused". Look at the next rows in that column until you find one that is blank. Shift all of those rows into indexed column 15.

        # Then look at the row number that is saved in "tempRow", look at the next columns in that row until you find the 2nd column that has "Excused". Look at the next rows in that column until you find one that is blank. Shift all of those rows into indexed column 16.

        # Then repeat this for the rest of the document. 
        

                # ...existing code...

        def _get_cell(rows, r_idx, c_idx):
            if r_idx >= len(rows):
                return ""
            row = rows[r_idx]
            if c_idx >= len(row):
                return ""
            return str(row[c_idx]).strip()

        def _set_cell(rows, r_idx, c_idx, value):
            row = rows[r_idx]
            if c_idx >= len(row):
                row.extend([""] * (c_idx + 1 - len(row)))
            row[c_idx] = value

        def _shift_down_until_blank(rows, start_row, src_col, dst_col):
            """
            Starting at start_row, move values from src_col to dst_col
            until a blank cell is found in src_col.
            """
            for r in range(start_row, len(rows)):
                val = _get_cell(rows, r, src_col)
                if val == "":
                    break
                _set_cell(rows, r, dst_col, val)
                _set_cell(rows, r, src_col, "")

        # Find each section where column index 6 starts with "Name"
        for tempRow in range(len(raw_rows)):
            if not _get_cell(raw_rows, tempRow, 6).lower().startswith("name"):
                continue

            header_row = raw_rows[tempRow]
            lowered = [str(x).strip().lower() for x in header_row]

            # Source columns discovered from the tempRow header
            enrolled_cols = [i for i, v in enumerate(lowered) if "enrolled" in v]
            excused_cols = [i for i, v in enumerate(lowered) if "excused" in v]
            present_col = next((i for i, v in enumerate(lowered) if "present" in v), None)

            # 1) Name block: col 6 -> col 4
            _shift_down_until_blank(raw_rows, tempRow + 1, 6, 4)

            # 2) First "Enrolled": -> col 10
            if len(enrolled_cols) >= 1:
                _shift_down_until_blank(raw_rows, tempRow + 1, enrolled_cols[0], 10)

            # 3) Second "Enrolled": -> col 12
            if len(enrolled_cols) >= 2:
                _shift_down_until_blank(raw_rows, tempRow + 1, enrolled_cols[1], 12)

            # 4) "Present": -> col 13
            if present_col is not None:
                _shift_down_until_blank(raw_rows, tempRow + 1, present_col, 13)

            # 5) First "Excused": -> col 15
            if len(excused_cols) >= 1:
                _shift_down_until_blank(raw_rows, tempRow + 1, excused_cols[0], 15)

            # 6) Second "Excused": -> col 16
            if len(excused_cols) >= 2:
                _shift_down_until_blank(raw_rows, tempRow + 1, excused_cols[1], 16)

        # ...existing code...



        # Skip the first 5 rows
        raw_rows = raw_rows[5:]

        cleaned_rows = []
        for row in raw_rows:
            # Clear the 3rd, 7th, 8th, and 9th columns if they exist
            for idx in [1,2,3,5,6,7,8,9,11,14]:  # zero-based indexing (3rd, 7th, 8th, 9th)
                if idx < len(row):
                    row[idx] = ''
            # Remove empty cells (after stripping whitespace)
            new_row = [cell.strip() for cell in row if cell.strip() != '']
            if new_row:  # keep only rows with at least one non-empty cell
                cleaned_rows.append(new_row)

        # Rename the first row as headers
        headers = ["id","name","daysEnrolled","daysNotEnrolled","daysPresent","daysExcused","daysNotExcused","drop","drop","drop"]
        if cleaned_rows:
            cleaned_rows[0] = headers

        # Write cleaned rows back to the same file
        with open(csv_path, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile)
            writer.writerows(cleaned_rows)

        print(Fore.GREEN + f"File '{csv_file}' cleaned and saved in place (empty cells removed).")


# Clean the data
def cleanData(df):
    entryID = getEntryID()
    week = getWeek()

    # 3) Delete any row where the first cell (id) is not an 8-digit number,
    id_str = df["id"].astype(str).str.strip()
    mask_8_digit = id_str.str.fullmatch(r"\d{8}").fillna(False)
    df = df[mask_8_digit].copy()

    # 4) Add columns for entryID and week
    df["entryID"] = int(entryID)
    df["week"] = week

    # 5) Ensure numeric types: id -> int, attendance fields -> float
    #    Keep header row safe by re-deriving masks post-filter
    id_str = df["id"].astype(str).str.strip()
    mask_8_digit = id_str.str.fullmatch(r"\d{8}").fillna(False)
    # Convert only valid ID rows to int; leave header row as-is (string)
    df.loc[mask_8_digit, "id"] = id_str[mask_8_digit].astype(int)

    numeric_cols = [
        "daysEnrolled",
        "daysNotEnrolled",
        "daysPresent",
        "daysExcused",
        "daysNotExcused",
    ]
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype(float)

    # 6) Reorder columns to match the BigQuery schema
    ordered_cols = [
        "entryID",
        "week",
        "id",
        "name",
        "daysEnrolled",
        "daysNotEnrolled",
        "daysPresent",
        "daysExcused",
        "daysNotExcused",
    ]
    df = df[ordered_cols]

    return df

def doWork():
    """
    Main function for the script.
    """
    print(Fore.YELLOW + f"Starting {scriptName} script...")
    print(Fore.YELLOW + "MAKE SURE YOU FORMATTED THE LAST SECTION!")

    prepareCSVFile(source_folder)

    # # Read in the first .csv file found
    csv_file, rawDataFrame = readCSV(source_folder)

    # # Clean the data
    cleanedDataFrame = cleanData(rawDataFrame)

    # # Upload the data to BigQuery
    uploadToBigQuery(cleanedDataFrame, tableSchema, project_id, dataset_id, table_id)

    # # Archive the source file
    archiveSourceFile(cleanedDataFrame, csv_file, source_folder, table_id)

    print(Fore.GREEN + f"{scriptName} data pull complete.")


doWork()

