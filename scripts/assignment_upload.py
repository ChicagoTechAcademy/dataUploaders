from _dataManager import *
import os


scriptName = "Assignment Breakdown by Letter Grade Report"


# Constants
project_id = "chitechdb"
dataset_id = "academics"
table_id = "assignments"
source_folder = f"../dataUploaders/{table_id}"

# entryID	term	teacher	class	period	assignment	num_A	num_B	num_C	num_D	num_F	num_blank	special_codes	num_grades
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
 
]

# Mapping of original column names to new names
# Teacher,	Class,		Period,	Assignment,		# of As,	# of Bs,	# of Cs,	# of Ds,	# of Fs,	# of /s,	"special codes",	# of grades
COLUMN_MAPPINGS = {
    0: "teacher",
    1: "class",
    2: "period",
    3: "assignment",
    4: "num_A",
    5: "num_B",
    6: "num_C",
    7: "num_D",
    8: "num_F",
    9: "num_blank",
    10: "special_codes",
    11: "num_grades",
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

# get term, make sure it's in the format Q1, Q2, Q3, Q4. if lowercase, make it caps
def getTerm():
    # term = input(Fore.CYAN + "Enter the term for this roster (e.g., Q1, Q2, Q3, Q4): ")
    # return term.upper()
    return "-"


def prepareCSVFile(source_folder):
    # open and delete the first row of the first csv file found in the source folder
    for file in os.listdir(source_folder):
        if file.endswith(".csv"):
            file_path = os.path.join(source_folder, file)
            with open(file_path, 'r') as f:
                lines = f.readlines()
            with open(file_path, 'w') as f:
                f.writelines(lines[1:])  # write all lines except the first
            print(Fore.GREEN + f"Prepared CSV file: {file}")
            return file_path
    print(Fore.RED + "No CSV files found in the source folder.")
    return None

def cleanData(df):
    entryID = getEntryID()

    print("Cleaning data...")

    # delete all empty cells, and shift all cells to the left if there are any empty cells
    df = df.apply(lambda x: pd.Series(x.dropna().values), axis=1)
    df = df.dropna(axis=1, how='all')  # drop any columns that are completely empty
    df = df.reset_index(drop=True)  # reset the index after dropping rows/columns

    # delete all rows where the first column contains "Totals"
    df = df[~df[0].str.contains("Totals", case=False, na=False)]

    # Rename columns based on predefined mappings
    df.rename(columns=COLUMN_MAPPINGS, inplace=True, errors="raise")
    
    # go through the period column, and if it's a 1, then "01(Reg)", if it's a 2, then "02", if it's a 3, then "03", if it's a 4, then "04", if it's a 5, then "05", if it's a 6, then "06", if it's a 7, then "07", if it's an 8, then "08", if it's a 9, then "09", if it's a 10, then "10", if it's an 11, then "11", if it's a 12, then "12". If it's already in that format, leave it alone.
    period_mapping = {
        "01": "01(Reg)",
        "02": "02(Reg)",
        "03": "03(Reg)",
        "04": "04(Reg)",
        "05": "05(Reg)",
        "06": "06(Reg)",
        "07": "07(Reg)",
        "08": "08(Reg)",
        "Lu": "Lunch/Adv(Reg)",
        "Ad": "Adv/Lunch(Reg)"
    }   
    df["period"] = df["period"].map(period_mapping).fillna(df["period"])
    # If there are any columns not in the mapping, drop them
    df = df.reset_index(drop=True)  # reset the index after dropping rows/columns

    # Add 'entryID' and term columns
    df['entryID'] = entryID
    df['term'] = getTerm()

    # ensure all data types are correct
    df["entryID"] = df["entryID"].astype(int)
    df["teacher"] = df["teacher"].astype(str)
    df["class"] = df["class"].astype(str)
    df["period"] = df["period"].astype(str)
    df["assignment"] = df["assignment"].astype(str)
    df["num_A"] = df["num_A"].astype(int)
    df["num_B"] = df["num_B"].astype(int)
    df["num_C"] = df["num_C"].astype(int)
    df["num_D"] = df["num_D"].astype(int)
    df["num_F"] = df["num_F"].astype(int)
    df["num_blank"] = df["num_blank"].astype(int)
    df["special_codes"] = df["special_codes"].astype(int)
    df["num_grades"] = df["num_grades"].astype(int)

    return df


def doWork():
    """
    Main function for the script.
    """
    print(Fore.YELLOW + f"Starting {scriptName} script...")

    prepareCSVFile(source_folder)

    # Read in the first .csv file found
    csv_file, rawDataFrame = readCSV(source_folder)

    # Clean the data
    cleanedDataFrame = cleanData(rawDataFrame)

    # Upload the data to BigQuery
    uploadToBigQuery(cleanedDataFrame, schema, project_id, dataset_id, table_id)

    # Archive the source file
    archiveSourceFile(cleanedDataFrame, csv_file, source_folder, table_id)

    print(Fore.GREEN + f"{scriptName} data pull complete.")


doWork()
