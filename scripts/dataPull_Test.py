import pandas_gbq
import pydata_google_auth
import os
from google.cloud import bigquery
from datetime import datetime
from colorama import Fore

# Constants
project_id = "chitechdb"
table_id = "student_info.accountBalances"
roster_table_id = "student_info.roster"
source_folder = "../dataUploaders/accountBalances"
destination_folder = "../dataUploaders/archivedFiles"
archive_name = "accountBalances"

SCOPES = [
    'https://www.googleapis.com/auth/cloud-platform',
    'https://www.googleapis.com/auth/drive',
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

def fetch_roster_data(ids):
    """
    Queries BigQuery for the name and YOG based on a list of IDs.
    Returns a DataFrame.
    """
    query = f"SELECT id, name, yog FROM `{roster_table_id}` WHERE id IN {tuple(ids)}"
    roster_df = pandas_gbq.read_gbq(
        query,
        project_id='chitechdb',
        credentials=credentials,
    )
    print(roster_df)
    return roster_df

def read_gbq():
    df = pandas_gbq.read_gbq(
        "SELECT * FROM `student_info.medicalCompliance`",
        project_id='chitechdb',
        credentials=credentials,
    )

    print(df)

print(fetch_roster_data([50208927,50257694,60183163]))

