import os
import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from datetime import datetime
import re

# Constants
project_id = "chitechdb"
table_id = "academics.transcripts"
roster_table_id = "student_info.roster"
catalog_table_id = "logistics.cps_course_catalog"
source_folder = "../dataUploaders/transcripts"
destination_folder = "../dataUploaders/archivedFiles"
archive_name = "transcripts"

# Desired column names
column_names = [
    "name", "id", "drop9", "gradeTaken", "courseCode","courseName","creditsEarned","term","finalGrade","courseCodeAndSection","teacher","creditsPossible","school","drop1","drop2","drop3","drop4","drop4","drop5","drop6","drop7","drop8",
]

def update_term_column(df):
    """
    Look through "courseName" for the 3rd value.
    If it's a 1, put "S1" in term for that row.
    If it's a 2, put "S2" in term for that row.
    If it's neither, then leave it empty.
    """
    def get_term(course_name):
        try:
            term_value = course_name[2]  # Get the third character
            if term_value == '1':
                return "S1"
            elif term_value == '2':
                return "S2"
            else:
                return ""
        except IndexError:
            return ""
        
    df["term"] = df["courseName"].apply(get_term)
    return df

# Adjusted function to fetch course data
def fetch_course_data(course_codes):
    """
    Queries BigQuery for course details based on a list of course codes.
    Returns a DataFrame.
    """
    client = bigquery.Client(project=project_id)
    query = f"""
    SELECT Title, S1_Course_Number, S2_Course_Number, Credit_Earned_1, Credit_Earned_2, 
           Credit_Earned_3, Credit_Earned_4, Credit_Earned_5
    FROM `chitechdb.logistics.cps_course_catalog` 
    WHERE S1_Course_Number IN {tuple(course_codes)} OR S2_Course_Number IN {tuple(course_codes)}
    """
    course_df = client.query(query).to_dataframe()
    return course_df

def clean_data(df):
    print("Cleaning data...")

    # Rename columns
    df.columns = column_names
    
    # Drop unwanted columns
    df = df[["name", "id","gradeTaken", "courseCode", "courseName","creditsPossible","finalGrade",  "teacher", "creditsEarned","school","courseCodeAndSection", "term"]]

    # delete rows based on "finalGrade". If it's a "/", "F", or is empty, delete the row
    df = df[df["finalGrade"] != "/"]
    df = df[df["finalGrade"] != "F"]
    df = df[df["finalGrade"].notna()]
    
    df = update_term_column(df)  # Update the term column

    #  Get distinct course codes from the cleaned data
    course_codes = df["courseCode"].unique()
    course_df = fetch_course_data(course_codes)

    # Merge the main df with course_df on courseCode against S1_Course_Number and S2_Course_Number
    df = pd.merge(df, course_df, how="left", left_on="courseCode", right_on="S1_Course_Number")
    df = pd.merge(df, course_df, how="left", left_on="courseCode", right_on="S2_Course_Number", suffixes=('', '_S2'))

    # For rows where 'Title' is blank, set values from "_S2" columns
    mask = df['Title'].isna() | (df['Title'] == "")
    for col in ["Title", "S1_Course_Number", "S2_Course_Number", "Credit_Earned_1", "Credit_Earned_2", "Credit_Earned_3", "Credit_Earned_4", "Credit_Earned_5"]:
        df.loc[mask, col] = df[col + "_S2"]

    # Drop the "_S2" columns as they are no longer needed
    df.drop(columns=["Title_S2", "S1_Course_Number_S2", "S2_Course_Number_S2", "Credit_Earned_1_S2", "Credit_Earned_2_S2", "Credit_Earned_3_S2", "Credit_Earned_4_S2", "Credit_Earned_5_S2"], inplace=True)


    print("Merging data...")
    return df

def fetch_roster_data(ids):
    """
    Queries BigQuery for the name and YOG based on a list of IDs.
    Returns a DataFrame.
    """
    client = bigquery.Client(project=project_id)
    query = f"SELECT id, name, yog FROM `{roster_table_id}` WHERE id IN {tuple(ids)}"
    roster_df = client.query(query).to_dataframe()
    return roster_df


def delete_all_data_from_table():
    """
    Deletes all data from the specified BigQuery table.
    """
    print("Deleting old data from database to override...")
    client = bigquery.Client(project=project_id)
    query = f"DELETE FROM `{table_id}` WHERE TRUE"
    client.query(query).result()  # Execute the DELETE query
    print(f"All data from {table_id} has been deleted.")

def archive_source_file(csv_file, df):
    current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    new_file_name = f"{archive_name}-{current_datetime}.csv"
    destination_file_path = os.path.join(destination_folder, new_file_name)
    df.to_csv(destination_file_path, index=False)
    os.remove(os.path.join(source_folder, csv_file))
    print(f"File '{csv_file}' has been archived as '{new_file_name}'.")

def upload_to_big_query(df):
    # Define the schema for the BigQuery table
    schema = [
        {"name": "name", "type": "STRING"},
        {"name": "id", "type": "INTEGER"},
        {"name": "gradeTaken", "type": "INTEGER"},
        {"name": "courseCode", "type": "STRING"},
        {"name": "courseName", "type": "STRING"},
        {"name": "creditsPossible", "type": "FLOAT"},
        {"name": "finalGrade", "type": "STRING"},
        {"name": "teacher", "type": "STRING"},
        {"name": "creditsEarned", "type": "FLOAT"},
        {"name": "school", "type": "STRING"},
        {"name": "courseCodeAndSection", "type": "STRING"},
        {"name": "term", "type": "STRING"},
        {"name": "Title", "type": "STRING"},
        {"name": "S1_Course_Number", "type": "STRING"},
        {"name": "S2_Course_Number", "type": "STRING"},
        {"name": "Credit_Earned_1", "type": "STRING"},
        {"name": "Credit_Earned_2", "type": "STRING"},
        {"name": "Credit_Earned_3", "type": "STRING"},
        {"name": "Credit_Earned_4", "type": "STRING"},
        {"name": "Credit_Earned_5", "type": "STRING"},
    ]

    print("Uploading data to BigQuery...")
    # Upload the dataframe
    pandas_gbq.to_gbq(df, destination_table=table_id, project_id=project_id,
                      if_exists="append", progress_bar=True, table_schema=schema)


if __name__ == "__main__":
    print("Starting script...")

    csv_files = [f for f in os.listdir(source_folder) if f.endswith('.csv')]
    
    if csv_files:
        csv_file = csv_files[0]
        csv_path = os.path.join(source_folder, csv_file)

        df = pd.read_csv(csv_path).dropna(how="all")
        print("Deleted empty rows from the DataFrame.")
        
        df = clean_data(df)
        
        delete_all_data_from_table()  # Call this function before uploading new data
        upload_to_big_query(df)
        archive_source_file(csv_file, df)
    else:
        print(f"No CSV files found in the '{archive_name}' folder.")