import csv
import pandas as pd
import pandas_gbq
from google.cloud import bigquery

# Define your GCP project ID and BigQuery dataset ID
project_id = "chitechdb"
dataset_id = "attendance"
table_id = "weekly-avg-daily-att"

# Set up BigQuery client
client = bigquery.Client()

# Prompt the user for the week number
week_number = input("Enter the week number (e.g., W01): ")

file_name = "weekly-avg-daily-att.csv"

# Open the CSV file for reading
with open(file_name, 'r') as csv_file:
    # Create a CSV reader
    reader = csv.reader(csv_file)
    # Create a list to store the cleaned rows
    cleaned_rows = []
    # Add the header row to the list of cleaned rows
    cleaned_rows.append(["id", "name", "daysEnrolled", "daysNotEnrolled", "daysPresent", "daysExcused", "daysNotExcused", "week", "sy", "attPercentage", "excusedPercentage"])
    # Iterate through all rows in the CSV file
    for row in reader:
        # Check if the first cell in the row is a number
        if row and row[0].isdigit():
            # Check if the 4th cell is blank
            if row[3]:
                # Keep only the specified cells from the row
                daysEnrolled = float(row[9])
                daysPresent = float(row[12])
                daysExcused = float(row[14])
                attPercentage = (daysPresent / daysEnrolled) * 100 if daysEnrolled != 0 else 0
                excusedPercentage = ((daysPresent + daysExcused) / daysEnrolled) * 100 if daysEnrolled != 0 else 0
                cleaned_row = [row[0], row[3], daysEnrolled, row[11], daysPresent, daysExcused, row[15], week_number, "SY24", attPercentage, excusedPercentage]
            else:
                # Check if the 22nd cell is blank
                if row[21]:
                    # Keep only the specified cells from the row
                    daysEnrolled = float(row[16])
                    daysPresent = float(row[21])
                    daysExcused = float(row[23])
                    attPercentage = (daysPresent / daysEnrolled) * 100 if daysEnrolled != 0 else 0
                    excusedPercentage = ((daysPresent + daysExcused) / daysEnrolled) * 100 if daysEnrolled != 0 else 0
                    cleaned_row = [row[0], row[5], daysEnrolled, row[18], daysPresent, daysExcused, row[24], week_number, "SY24", attPercentage, excusedPercentage]
                else:
                    # Keep only the specified cells from the row
                    daysEnrolled = float(row[16])
                    daysPresent = float(row[23])
                    daysExcused = float(row[25])
                    attPercentage = (daysPresent / daysEnrolled) * 100 if daysEnrolled != 0 else 0
                    excusedPercentage = ((daysPresent + daysExcused) / daysEnrolled) * 100 if daysEnrolled != 0 else 0
                    cleaned_row = [row[0], row[5], daysEnrolled, row[18], daysPresent, daysExcused, row[26], week_number, "SY24", attPercentage, excusedPercentage]
            # Add the cleaned row to the list of cleaned rows
            cleaned_rows.append(cleaned_row)


# Open the CSV file for writing
with open(file_name, 'w', newline='') as csv_file:
    # Create a CSV writer
    writer = csv.writer(csv_file)
    # Write the cleaned rows to the CSV file
    writer.writerows(cleaned_rows)

print("Processing completed and saved to 'weekly-avg-daily-att.csv'")

# Define BigQuery dataset reference
dataset_ref = client.dataset(dataset_id)

# Load CSV data into BigQuery table
table_ref = dataset_ref.table(table_id)
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    autodetect=True,
)
with open(file_name, "rb") as source_file:
    job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

job.result()  # Wait for the job to complete

print("CSV data uploaded to BigQuery table")