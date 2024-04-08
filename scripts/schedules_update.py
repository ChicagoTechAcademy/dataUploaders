from _dataManager import *


scriptName = "schedule_update"


# Constants
project_id = "chitechdb"
dataset_id = "academics"
table_id = "schedules"
source_folder = f"../dataUploaders/{table_id}"

tableSchema = [
        {"name": "id", "type": "INTEGER"},
        {"name": "lastName", "type": "STRING"},
        {"name": "firstName", "type": "STRING"},
        {"name": "grade", "type": "INTEGER"},
        {"name": "classCode", "type": "STRING"},
        {"name": "class", "type": "STRING"},
        {"name": "room", "type": "STRING"},
        {"name": "teacher", "type": "STRING"},
        {"name": "period", "type": "STRING"},
        {"name": "yog", "type": "INTEGER"},
        {"name": "scheduleAsOf", "type": "DATE"},
        {"name": "sy", "type": "STRING"},
        {"name": "semester", "type": "STRING"},
    ]

column_mappings = {
    "LastName": "lastName",
    "FirstName": "firstName",
    "Student ID": "id",
    "Grade": "grade",
    "Class": "classCode",
    "Description": "class",
    "Clssrm": "room",
    "Name": "teacher",
    "Schedule": "period",
}


def cleanData(df):
    """
    Processes the dataframe for cleanup tasks.
    """

    print("Cleaning data...")

    # Drop unnecessary columns
    df.drop(
        columns=[
            "MiddleName",
            "SpecialEdStus",
            "Homeroom",
            "Inclusion?",
            "SecType",
            "Total",
            "Max",
        ],
        inplace=True,
        errors="ignore",
    )
    print("Dropped unnecessary columns.")

    # Rename columns based on predefined mappings
    df.rename(columns=column_mappings, inplace=True, errors="raise")
    print(f"Renamed columns to: {', '.join(column_mappings.values())}")

    # Add 'scheduleAsOf', 'sy', and 'semester' columns
    df["scheduleAsOf"] = datetime.now().strftime("%Y-%m-%d")
    df["sy"] = "SY24"

    # Get the semester by reading the 6th character of the classCode.
    # If it's a number 1, then set the semester to S1, if it's a number 2, then set the semester to S2. Otherwise drop the row.
    # df["semester"] = df["classCode"].apply(lambda x: "S1" if x[5] == "1" else "S2" if x[5] == "2" else None)
    
    # Get the classType by reading the 7th character of the classCode.
    # If it's the letter "N", then change the semester to "Other". Othwerwise, keep the semester as is.
    df["semester"] = df["classCode"].apply(lambda x: "Other" if x[6] == "N" else "S1" if x[5] == "1" else "S2" if x[5] == "2" else None)

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

    deleteAllDataFromTable(project_id, dataset_id, table_id)

    # Upload the data to BigQuery
    uploadToBigQuery(cleanedDataFrame, tableSchema, project_id, dataset_id, table_id)

    # Archive the source file
    archiveSourceFile(cleanedDataFrame, csv_file, source_folder, table_id)


    print(Fore.GREEN + f"{scriptName} data pull complete.")


doWork()

