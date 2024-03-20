from _dataManager import *
import os


scriptName = "focus_sweep_update"


# Constants
project_id = "chitechdb"
dataset_id = "attendance"
table_id = "focus-sweep"
source_folder = f"../dataUploaders/{table_id}"

schema = [
        {"name": "id", "type": "INTEGER"},
        {"name": "name", "type": "STRING"},
        {"name": "status", "type": "STRING"},
        {"name": "period", "type": "STRING"},
        {"name": "row_number", "type": "INTEGER"},
        {"name": "student_tardy_count", "type": "INTEGER"},
        {"name": "date", "type": "DATE"},
        {"name": "time", "type": "TIME"},
    ]

# Mapping of original column names to new names
column_mappings = {
    "Swipe_Time": "swipe_time",
    "Student_ID": "id",
    "Division": "division",
    "Student_Name": "name",
    "Status": "status",
    "Period_Cause": "period",
    "Row_Number": "row_number",
    "Textbox6": "student_tardy_count",
}


def cleanData(df):
    """
    Processes the dataframe for cleanup tasks.
    """

    print("Cleaning data...")

    # Rename columns based on predefined mappings
    df.rename(columns=column_mappings, inplace=True, errors="raise")

    # Convert swipe_time to date and time columns
    df["date"] = df["swipe_time"].str.split(" ").str[0].apply(convertToStandardDate)
    df["time"] = df["swipe_time"].str.split(" ").str[1]

    # Drop unnecessary columns
    df.drop(columns=["swipe_time", "division"], inplace=True, errors="ignore")

    # Convert id, row_number, and student_tardy_count to integers
    df["id"] = df["id"].astype(int)
    df["row_number"] = df["row_number"].astype(int)
    df["student_tardy_count"] = df["student_tardy_count"].astype(int)

    #Convert name, status, and period to strings
    df["name"] = df["name"].astype(str)
    df["status"] = df["status"].astype(str)
    df["period"] = df["period"].astype(str)

    #Convert date and time to date and time types
    df["date"] = pd.to_datetime(df["date"])
    df["time"] = pd.to_datetime(df["time"])

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

def remove_first_three_lines(csv_path):
    """
    Removes the first three lines from a CSV file.
    """
    with open(csv_path, "r") as file:
        data = file.readlines()[3:]

    with open(csv_path, "w") as file:
        file.writelines(data)




def doWork():
    """
    Main function for the script.
    """
    print(f"Starting {scriptName} script...")

    temp = find_csv_file(source_folder)
    remove_first_three_lines(temp)

    # Read in the first .csv file found
    csv_file, rawDataFrame = readCSV(source_folder)

    # Clean the data
    cleanedDataFrame = cleanData(rawDataFrame)

    deleteAllDataFromTable(project_id, dataset_id, table_id)

    # Upload the data to BigQuery
    uploadToBigQuery(cleanedDataFrame, schema, project_id, dataset_id, table_id)

    # Archive the source file
    archiveSourceFile(cleanedDataFrame, csv_file, source_folder, table_id)


doWork()
