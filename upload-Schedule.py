import pandas as pd
import pandas_gbq
from google.cloud import bigquery

# Define your GCP project ID and BigQuery dataset ID
project_id = "chitechdb"
dataset_id = "academics"
table_id = "academics.schedules"

# Initialize the BigQuery client
client = bigquery.Client(project=project_id)
print(client)

# Define the column name mappings
column_mappings = {
    "LastName": "lastName",
    "FirstName": "firstName",
    "Student ID": "id",
    "Grade": "grade",
    "Class": "classCode",
    "Description": "class",
    "Clssrm": "room",
    "Name": "teacher",
    "Schedule": "period"
}

# Read the CSV file
file_path = "schedule.csv"
df = pd.read_csv(file_path)

# Remove columns from the DataFrame
columns_to_remove = ["MiddleName", "SpecialEdStus", "Homeroom", "Inclusion?", "SecType", "Total", "Max"]
df = df.drop(columns=columns_to_remove)

# Rename the columns that need renaming
df.rename(columns=column_mappings, inplace=True)

# Calculate the "sy" column based on the year and month
def get_yog(grade):
    if grade == 9:
        return 2027
    elif grade == 10:
        return 2026
    elif grade == 11:
        return 2025
    elif grade == 12:
        return 2024
    return str(grade)  # Convert year to string

# Add the "yog" column if it doesn't exist
if "yog" not in df.columns:
    df["yog"] = df["grade"].apply(get_yog)

# Add the "sy" and "semester" columns
df["sy"] = "SY24"
df["semester"] = "S1"

# Save the DataFrame to a temporary CSV file
temp_csv_path = "temp_schedule.csv"
df.to_csv(temp_csv_path, index=False)

# Construct the SQL query to delete old data
query = f"""
-- Delete old data
DELETE FROM `{project_id}.{table_id}`
WHERE 1=1;
"""

# Execute the delete query
query_job = client.query(query)
query_job.result()

print("Data deletion completed.")

# Upload the DataFrame to BigQuery from the temporary CSV file
pandas_gbq.to_gbq(
    pd.read_csv(temp_csv_path),
    destination_table=table_id,
    project_id=project_id,
    if_exists="append",
    table_schema=[
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
        {"name": "sy", "type": "STRING"},
        {"name": "semester", "type": "STRING"},
    ],
    progress_bar=True,
)

print("Data uploaded to BigQuery table.")
