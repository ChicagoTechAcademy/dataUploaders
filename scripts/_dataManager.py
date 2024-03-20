from datetime import datetime
import pandas as pd
import pandas_gbq
import pydata_google_auth
import os
from colorama import init, Fore
init(autoreset=True)


SCOPES = [
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/drive",
]

credentials = pydata_google_auth.get_user_credentials(
    SCOPES,
    # Note, this doesn't work if you're running from a notebook on a
    # remote sever, such as over SSH or with Google Colab. In those cases,
    # install the gcloud command line interface and authenticate with the
    # `gcloud auth application-default login` command and the `--no-browser`
    # option.
    auth_local_webserver=True,
)

def fetchDataFromBigQuery(query):
    """
    Fetches data from BigQuery and returns a DataFrame.
    """
    print(Fore.BLUE + "Fetching data from BigQuery...")
    df = pandas_gbq.read_gbq(
        query,
        project_id="chitechdb",
        credentials=credentials,
    )
    return df

def deleteAllDataFromTable(project_id, dataset_id, table_id):
    """
    Deletes all data from the specified BigQuery table.
    """
    print(Fore.BLUE + "Deleting old data from database to override...")
    pandas_gbq.context.credentials = credentials
    pandas_gbq.context.project = project_id
    query = f"DELETE FROM `{dataset_id}.{table_id}` WHERE TRUE"
    pandas_gbq.read_gbq(query)
    print(Fore.BLUE + f"All data from {table_id} has been deleted.")

def deleteDataBetweenDates(project_id, dataset_id, table_id, min_date, max_date):
    """
    Deletes data from the specified BigQuery table within a specified date range.
    """
    print(Fore.BLUE + "Deleting data from database within date range...")
    pandas_gbq.context.credentials = credentials
    pandas_gbq.context.project = project_id
    query = f"DELETE FROM `{dataset_id}.{table_id}` WHERE date BETWEEN '{min_date}' AND '{max_date}'"
    pandas_gbq.read_gbq(query)
    print(Fore.BLUE + "Data deletion completed.")


def uploadToBigQuery(df, schema, project_id, dataset_id, table_id):
    """
    Uploads a pandas DataFrame to a BigQuery table.
    """

    print(Fore.BLUE + "Uploading data to BigQuery...")
    # Upload the dataframe
    pandas_gbq.to_gbq(
        df,
        destination_table=dataset_id + "." + table_id,
        project_id=project_id,
        if_exists="append",
        progress_bar=True,
        table_schema=schema,
    )
    print(Fore.BLUE + "Data has been uploaded to BigQuery.")


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
    year = int(date.split('-')[0])
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