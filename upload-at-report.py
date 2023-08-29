import pandas as pd
import pandas_gbq
from google.cloud import bigquery

# Define your GCP project ID and BigQuery dataset ID
project_id = "chitechdb"
dataset_id = "attendance"
table_id = "attendance.at-report"

# Initialize the BigQuery client
client = bigquery.Client(project=project_id)
print(client)
# Define the column name mappings
column_mappings = {
    "Student > Name": "name",
    "Student > Student ID": "id",
    "Date": "date",
    "Code": "code",
    "Master > Class": "course",
    "Master > Description": "class",
    "Period": "period",
    "Tardy?": "tardy",
    "Absent?": "absent",
    "Student > YOG": "yog",
}

# Read the CSV file
file_path = "at-report.csv"
df = pd.read_csv(file_path)

# Remove empty rows from the DataFrame
df = df.dropna(how="all")

# Rename the columns that need renaming
for old_col, new_col in column_mappings.items():
    if old_col in df.columns and new_col not in df.columns:
        df.rename(columns={old_col: new_col}, inplace=True)
        print(f"Success! Renamed {old_col} to {new_col}")


# Convert the "date" column to "YYYY-MM-DD" format
def convert_to_standard_date(date_str):
    month, day, year = map(int, date_str.split("/"))
    if year < 100:
        if year >= 22:
            year += 2000
        else:
            year += 1900
    return f"{year:04d}-{month:02d}-{day:02d}"

df["date"] = df["date"].apply(convert_to_standard_date)

# Calculate the "sy" column based on the year and month
def get_sy(date):
    year, month, _ = map(int, date.split("-"))
    if (year == 2022 and month >= 8) or (year == 2023 and month <= 7):
        return "SY23"
    elif (year == 2023 and month >= 8) or (year == 2024 and month <= 7):
        return "SY24"
    return str(year)  # Convert year to string

# Check and add the "sy" column if it doesn't exist
if "sy" not in df.columns:
    df["sy"] = df["date"].apply(get_sy)

# Calculate the "semester" column based on the month
def get_semester(date):
    _, month, _ = map(int, date.split("-"))
    if month >= 8:
        return "S1"
    elif month >= 1:
        return "S2"
    return ""

# Check and add the "semester" column if it doesn't exist
if "semester" not in df.columns:
    df["semester"] = df["date"].apply(get_semester)

# Save the DataFrame to a CSV file
df.to_csv(file_path, index=False)

# Fetch the min and max dates from the DataFrame
min_date = df["date"].min()
print(min_date)
max_date = df["date"].max()
print(max_date)

# Construct the SQL query
query = f"""
DECLARE start_date DATE DEFAULT '{min_date}';
DECLARE end_date DATE DEFAULT '{max_date}';

-- Delete data between the specified dates
DELETE FROM `chitechdb.attendance.at-report`
WHERE date >= start_date AND date <= end_date;
""".format(project_id, dataset_id)

# Execute the query
query_job = client.query(query)
query_job.result()

print("Data deletion completed.")

# Execute the query
query_job = client.query(query)
query_job.result()

# # Upload the DataFrame to BigQuery
pandas_gbq.to_gbq(
    df,
    destination_table=table_id,
    project_id=project_id,
    if_exists="append",
    table_schema=[
        {"name": "name", "type": "STRING"},
        {"name": "id", "type": "INTEGER"},
        {"name": "date", "type": "DATE"},
        {"name": "code", "type": "STRING"},
        {"name": "course", "type": "STRING"},
        {"name": "class", "type": "STRING"},
        {"name": "period", "type": "STRING"},
        {"name": "tardy", "type": "BOOLEAN"},
        {"name": "absent", "type": "BOOLEAN"},
        {"name": "yog", "type": "INTEGER"},
        {"name": "sy", "type": "STRING"},
        {"name": "semester", "type": "STRING"},
    ],
    progress_bar=True,
)

print("Data uploaded to BigQuery table.")

# Wait for user input before closing the terminal
input("Press Enter to close the terminal...")

