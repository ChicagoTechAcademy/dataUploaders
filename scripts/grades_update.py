from _dataManager import *
from datetime import date


scriptName = "grades_update"


# Constants
project_id = "chitechdb"
dataset_id = "academics"
table_id = "grades"
source_folder = f"../dataUploaders/{table_id}"

tableSchema = [
    {"name": "entryID", "type": "INTEGER"},
    {"name": "week", "type": "STRING"},
    {"name": "date", "type": "DATE"},
    {"name": "id", "type": "INTEGER"},
    {"name": "name", "type": "STRING"},
    {"name": "yog", "type": "INTEGER"},
    {"name": "course", "type": "STRING"},
    {"name": "teacher", "type": "STRING"},
    {"name": "gradePercent", "type": "FLOAT"},
    {"name": "letterGrade", "type": "STRING"},
    {"name": "sy", "type": "STRING"},
]

COLUMN_NAMES = [
    "yog",
    "name",
    "drop1",
    "drop2",
    "id",
    "course",
    "drop3",
    "teacher",
    "drop4",
    "averageAndLetter",
    "move"
]

# Get the entry ID from the user
def getEntryID():
    # entryID = input(Fore.CYAN + "Enter the entry ID for this roster YYYYMMDDX: ")

    # find todays date, and reformat it as YYYYMMDD,
    entryID = pd.Timestamp.now().strftime("%Y%m%d")

    print(Fore.CYAN + f"The EntryID is {entryID}.")

    return int(entryID)

def getWeek():
    return input(Fore.CYAN + "Enter the week for this roster (W01, W02, ...): ")



def cleanData(df):
    """
    Cleans and processes data.
    """
    print(Fore.RESET + "Cleaning data...")
    
    


    # change the column names
    df.columns = COLUMN_NAMES

    # without deleting any data in averageAndLetter, move all data in the move column into averageAndLetter
    df["averageAndLetter"] = df["averageAndLetter"].fillna(df["move"])
    df["move"] = None

    week_number = getWeek()

    # Processing
    df["id"] = pd.to_numeric(df["id"], errors="coerce")
    df.dropna(subset=["id"], inplace=True)


    df[["gradePercent", "letterGrade"]] = df["averageAndLetter"].str.split(expand=True)
    df.drop(columns=["averageAndLetter"], inplace=True)

    sy = "SY26"
    df["sy"] = f"{sy}"

    df["entryID"] = getEntryID()

    df["week"] = f"{week_number}"
    df["id"] = df["id"].astype(int)
    df["gradePercent"] = df["gradePercent"].astype(float)
    df = df[
        [
            "entryID",
            "week",
            "sy",
            "id",
            "name",
            "course",
            "gradePercent",
            "letterGrade"
        ]
    ]

    print(Fore.RESET + "Merging data...")
    return df

def find_csv_file(folder_path):
    """
    Finds the first .csv file in the given folder path.
    Returns the file path if found, otherwise returns None.
    """
    for file in os.listdir(folder_path):
        if file.endswith(".csv"):
            return os.path.join(folder_path, file)
    return None


def prepareCSVFile(source_folder):

    csv_path = find_csv_file(source_folder)
    # open the file and add headers col1, col2, col3.... to col11
    with open(csv_path, "r") as file:
        data = file.readlines()

    # look in the indexed 10th column until you find the first non-empty value. Look through the next rows of that column until you find the first empty value. Shift all of that data into the 9th column, starting at the same row as the first non-empty value. Then delete all data in the 10th column.
    for i in range(len(data)):
        row = data[i].strip().split(",")
        if len(row) > 9 and row[9].strip() != "":
            # Found the first non-empty value in the 10th column
            for j in range(i, len(data)):
                next_row = data[j].strip().split(",")
                if len(next_row) > 9 and next_row[9].strip() == "":
                    # Found the first empty value in the 10th column, stop shifting
                    break
                # Shift the value from the 10th column to the 9th column
                if len(next_row) > 8:
                    next_row[8] = next_row[9]
                else:
                    # If there are not enough columns, add empty columns until we can shift
                    while len(next_row) <= 8:
                        next_row.append("")
                    next_row[8] = next_row[9]
                # Clear the value in the 10th column
                if len(next_row) > 9:
                    next_row[9] = ""
                data[j] = ",".join(next_row) + "\n"
            break

    headers = [f"col{i}" for i in range(1, 12)]
    data.insert(0, ",".join(headers) + "\n")

    with open(csv_path, "w") as file:
        file.writelines(data)

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
    uploadToBigQuery(cleanedDataFrame, tableSchema, project_id, dataset_id, table_id)

    # Archive the source file
    archiveSourceFile(cleanedDataFrame, csv_file, source_folder, table_id)


    print(Fore.GREEN + f"{scriptName} data pull complete.")


doWork()
