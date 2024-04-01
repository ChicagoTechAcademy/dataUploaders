from _dataManager import *


scriptName = "refactorTest"


# Constants
project_id = "chitechdb"
dataset_id = "attendance"
table_id = "half-day-report"
source_folder = f"../dataUploaders/{table_id}"

tableSchema = [
    {"name": "date", "type": "DATE"},
    {"name": "name", "type": "STRING"},
    {"name": "grade", "type": "STRING"},
    {"name": "code", "type": "STRING"},
    {"name": "percentAbs", "type": "FLOAT"},
    {"name": "id", "type": "INTEGER"},
    {"name": "sy", "type": "STRING"},
    {"name": "semester", "type": "STRING"},
]

COLUMN_MAPPINGS = {
    "Student > Name": "name",
    "Date": "date",
    "Student > Grade": "grade",
    "Student > Homeroom": "homeroom",
    "Code": "code",
    "Time": "time",
    "Absent?": "absent",
    "Tardy?": "tardy",
    "Excused?": "excused",
    "PcntAbs": "percentAbs",
    "Other": "other",
}


def cleanData(df):
    print(Fore.BLUE + "BigQuery client initialized.")

    df = df.dropna(how="all")
    df.rename(columns=COLUMN_MAPPINGS, inplace=True)
    df = drop_unnecessary_columns(df)

    query = "SELECT name, id FROM `chitechdb.student_info.allStudents`"
    mapping_df = fetchDataFromBigQuery(query)
    pd.merge(df, mapping_df, on="name", how="left")

    df["date"] = df["date"].apply(convertToStandardDate)
    df["sy"] = df["date"].apply(getSchoolYear)
    df["semester"] = df["date"].apply(getSemester)

    return df


def drop_unnecessary_columns(df):
    columns_to_remove = ["time", "absent", "tardy", "excused", "other", "homeroom"]
    return df.drop(columns=columns_to_remove)


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
