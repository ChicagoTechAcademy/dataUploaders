import pandas as pd
import pandas_gbq
from google.cloud import bigquery

# Define your GCP project ID and BigQuery dataset ID
project_id = "chitechdb"
dataset_id = "attendance"
table_id = "attendance.half-day-report"

# Initialize the BigQuery client
client = bigquery.Client(project=project_id)

# Define the column name mappings
column_mappings = {
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

# Read the CSV file
file_path = "half-day-report.csv"  # Corrected file_path
df = pd.read_csv(file_path)

# Remove empty rows from the DataFrame
df = df.dropna(how="all")

# Rename the columns that need renaming
for old_col, new_col in column_mappings.items():
    if old_col in df.columns and new_col not in df.columns:
        df.rename(columns={old_col: new_col}, inplace=True)
        print(f"Success! Renamed {old_col} to {new_col}")

# Remove columns from the DataFrame
columns_to_remove = ["time", "absent", "tardy", "excused", "other", "homeroom"]
df = df.drop(columns=columns_to_remove)

# Query student IDs based on names from the students table
name_id_query = """
SELECT name, id FROM `chitechdb.student_info.allStudents`
"""
name_id_df = client.query(name_id_query).to_dataframe()

# Merge the name-id mapping with the original DataFrame
df = pd.merge(df, name_id_df, on="name", how="left")


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

# Check and add the "sy" column if it doesn't exist
def get_sy(date):
    year, month, _ = map(int, date.split("-"))
    if (year == 2022 and month >= 8) or (year == 2023 and month <= 7):
        return "SY23"
    elif (year == 2023 and month >= 8) or (year == 2024 and month <= 7):
        return "SY24"
    return str(year)  # Convert year to string

if "sy" not in df.columns:
    df["sy"] = df["date"].apply(get_sy)

# Delete rows where "sy" is not "SY24"
# df = df[df["sy"] == "SY24"]

# Calculate the "semester" column based on the month
def get_semester(date):
    _, month, _ = map(int, date.split("-"))
    if month >= 8:
        return "S1"
    elif month >= 1:
        return "S2"
    return ""

if "semester" not in df.columns:
    df["semester"] = df["date"].apply(get_semester)

# Add the "id" column from the merged DataFrame
# if "id" in df.columns:
#     df = df[["id"] + df.columns.tolist()]

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
DELETE FROM `chitechdb.attendance.half-day-report`
WHERE date >= start_date AND date <= end_date AND sy="SY24";
""".format(project_id, dataset_id)

# Execute the query
query_job = client.query(query)
query_job.result()

print("Data deletion completed.")


# Upload the DataFrame to BigQuery
pandas_gbq.to_gbq(
    df,
    destination_table=table_id,
    project_id=project_id,
    if_exists="append",
    table_schema=[
        {"name": "date", "type": "DATE"},
        {"name": "name", "type": "STRING"},
        {"name": "grade", "type": "STRING"},
        {"name": "code", "type": "STRING"},
        {"name": "percentAbs", "type": "FLOAT"},
        {"name": "id", "type": "INTEGER"},
        {"name": "sy", "type": "STRING"},
        {"name": "semester", "type": "STRING"},
    ],
    progress_bar=True,
)

print("Data uploaded to BigQuery table.")
