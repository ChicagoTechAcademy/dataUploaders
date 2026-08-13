from _dataManager import *


scriptName = "half_day_report_update"


# Constants
project_id = "chitechdb"
dataset_id = "attendance"
table_id = "half-day-report"
source_folder = f"../dataUploaders/{table_id}"

tableSchema = [
    {"name": "entryID", "type": "INTEGER"},
    {"name": "week", "type": "STRING"},
    {"name": "date", "type": "DATE"},
    {"name": "name", "type": "STRING"},
    {"name": "id", "type": "INTEGER"},
    {"name": "yog", "type": "INTEGER"},
    {"name": "code", "type": "STRING"},
    {"name": "excused", "type": "BOOLEAN"},
    {"name": "percentAbs", "type": "FLOAT"},
]

COLUMN_MAPPINGS = {
    "Date": "date",
    "Student > Name": "name",
    "Student ID": "id",
    "Student > Grade": "grade",
    "Code": "code",
    "Time": "time",
    "Absent?": "absent",
    "Tardy?": "tardy",
    "Excused?": "excused",
    "PcntAbs": "percentAbs",
    "Other": "other", 
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


def cleanData(df):
    entryID = getEntryID()
    week = getWeek()

    print("Cleaning data...")

    # Columns are named Date	Student > Name	Student > Grade	Student ID	Code	Time	Absent?	Tardy?	Excused?	PcntAbs	Other
    # Rename columns based on the mappings
    df.rename(columns=COLUMN_MAPPINGS, inplace=True)
    
    # get the year of graduation from the grade column and put it in a new column 'yog'. Then drop the grade column. Do this by mapping: 9 -> 2029, 10 -> 2028, 11 -> 2027, 12 -> 2026
    grade_to_yog = {
        9: 2029,
        10: 2028,
        11: 2027,
        12: 2026
    }
    df["yog"] = df["grade"].map(grade_to_yog)
    df.drop(columns=["grade"], inplace=True, errors="ignore")

    # Drop time, absent, tardy, other columns
    df.drop(columns=["time", "absent", "tardy", "other"], inplace=True, errors="ignore")

    # Add 'entryID' and 'week' columns
    df['entryID'] = entryID
    df['week'] = week

    # ensure all data types are correct
    df["entryID"] = df["entryID"].astype(int)
    df['week'] = df['week'].astype(str)
    df["date"] = df["date"].apply(convertToStandardDate)
    df["name"] = df["name"].astype(str)
    df["id"] = df["id"].astype(int)
    df["yog"] = df["yog"].astype(int)
    df["code"] = df["code"].astype(str)
    df["excused"] = df["excused"].astype(bool)
    df["percentAbs"] = df["percentAbs"].astype(float)

    return df


def doWork():
    """
    Main function for the script.
    """
    print(Fore.YELLOW + f"Starting {scriptName} script...")

    # Read in the first .csv file found
    csv_file, rawDataFrame = readCSV(source_folder)

    # Clean the data
    cleanedDataFrame = cleanData(rawDataFrame)

    min_date, max_date = cleanedDataFrame["date"].min(), cleanedDataFrame["date"].max()

    deleteDataBetweenDates(project_id, dataset_id, table_id, min_date, max_date)
    # deleteAllDataFromTable(project_id, dataset_id, table_id)

    # Upload the data to BigQuery
    uploadToBigQuery(cleanedDataFrame, tableSchema, project_id, dataset_id, table_id)

    # Archive the source file
    archiveSourceFile(cleanedDataFrame, csv_file, source_folder, table_id)

    print(Fore.GREEN + f"{scriptName} data pull complete.")


doWork()
