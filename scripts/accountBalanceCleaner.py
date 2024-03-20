from _dataManager import *


scriptName = "accountBalanceCleaner"


# Constants
project_id = "chitechdb"
dataset_id = "student_info"
table_id = "accountBalances"
source_folder = f"../dataUploaders/{table_id}"

schema = [
    {"name": "name", "type": "STRING"},
    {"name": "id", "type": "INTEGER"},
    {"name": "yog", "type": "INTEGER"},
    {"name": "balance", "type": "FLOAT"},
]

# Desired column names
column_names = [
    "id",
    "balance",
    "drop1",
    "drop2",
    "drop3",
    "drop4",
    "drop5",
    "drop6",
    "drop7",
    "drop8",
    "drop9",
    "drop10",
    "drop11",
    "drop12",
    "drop13",
    "drop14",
    "drop15",
]


def clean_data(df):
    print(Fore.WHITE + "Cleaning data...")

    # Rename columns
    df.columns = column_names

    df = df.reset_index(drop=True)  # Reset index
    current_id = (
        None  # This will store the value from "drop2" whenever we find "Student ID:"
    )

    for index in range(len(df)):
        if df.loc[index, "id"] == "Student ID:":
            current_id = df.loc[index, "drop2"]
        elif current_id is not None:
            df.loc[index, "id"] = current_id

    # Drop unwanted columns
    df = df[["id", "balance"]]

    # Filter rows based on 'bal'
    df = df[df["balance"].str.startswith("This is a current", na=False)]

    # Remove 'This is a current balance' from 'bal'
    # This is a current statement of your account.  The total amount due is  $ 150.00  and is payable upon the indicated date.
    df["balance"] = df["balance"].str.replace(
        "This is a current statement of your account.  The total amount due is  $ ", ""
    )
    df["balance"] = df["balance"].str.replace(
        "  and is payable upon the indicated date.  ", ""
    )

    # if the balance column has ( ) around it, then convert to a negative number
    df["balance"] = df["balance"].apply(
        lambda x: x.replace("(", "-").replace(")", "") if "(" in x else x
    )

    # convert the balance column to float
    df["balance"] = df["balance"].astype(float)

    print(Fore.BLUE + "Getting names and YOG from data base...")
    # # Fetch student information from roster
    unique_ids = df["id"].dropna().unique().astype(int)  # Convert IDs to integers
    roster_df = fetchDataFromBigQuery(
        f"SELECT name, id, yog FROM `student_info.roster` WHERE id IN ({', '.join(map(str, unique_ids))})"
    )
    # # Merge roster data to df based on 'id'
    df = pd.merge(df, roster_df, on="id", how="left")

    # # Reorder columns
    df = df[["name", "id", "yog", "balance"]]

    print(Fore.WHITE + "Merging data...")
    return df


def doWork():
    print(Fore.YELLOW + f"Starting {scriptName} script...")

    # Read in the first .csv file found
    csv_file, rawDataFrame = readCSV(source_folder)

    # Clean the data
    cleanedDataFrame = clean_data(rawDataFrame)

    deleteAllDataFromTable(project_id, dataset_id, table_id)

    # Upload the data to BigQuery
    uploadToBigQuery(cleanedDataFrame, schema, project_id, dataset_id, table_id)

    # Archive the source file
    archiveSourceFile(cleanedDataFrame, csv_file, source_folder, table_id)


doWork()
