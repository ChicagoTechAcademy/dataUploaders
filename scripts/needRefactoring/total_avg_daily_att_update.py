import os
import csv
import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from datetime import datetime

# Define your GCP project ID and BigQuery dataset ID
project_id = "chitechdb"
dataset_id = "attendance"
table_id = "total-avg-daily-att"

# Specify the paths
source_folder = "../dataUploaders/total-avg-att"
destination_folder = "../dataUploaders/archivedFiles"
cleaned_file_name = "cleaned_total_avg_daily_att.csv"

# Prompt the user for the week number
week_number = input("Enter the week number (e.g., W01): ")


# Create a list to store the cleaned rows
cleaned_rows = []


# Function to get today's date and move the file to done&uploaded folder
def moveSourceFileToUsedFolder():
    # Generate the new file name with the date
    current_date = datetime.now().strftime("%Y-%m-%d")
    new_file_name = f"total-avg-att-{current_date}.csv"
    destination_file_path = os.path.join(destination_folder, new_file_name)

    # Open the CSV file for writing
    with open(destination_file_path, "w", newline="") as csv_file:
        # Create a CSV writer
        writer = csv.writer(csv_file)
        # Write the cleaned rows to the CSV file
        writer.writerows(cleaned_rows)
        # Delete the source file
        os.remove(source_file_path)

    print(
        f"File '{csv_file}' has been saved as '{new_file_name}' and moved to '{destination_folder}'."
    )


# List all files in the source folder
file_list = os.listdir(source_folder)

# Check if there is a .csv file in the folder
csv_files = [file for file in file_list if file.endswith(".csv")]

if csv_files:
    csv_file = csv_files[0]  # Get the first (and only) CSV file in the list
    source_file_path = os.path.join(source_folder, csv_file)


# Open the CSV file for reading
with open(source_file_path, "r") as csv_file:
    # Create a CSV reader
    reader = csv.reader(csv_file)

    client = bigquery.Client(project=project_id)

    if client:
        print("BigQuery client initialized.")

        # Add the header row to the list of cleaned rows
        cleaned_rows.append(
            [
                "id",
                "name",
                "daysEnrolled",
                "daysNotEnrolled",
                "daysPresent",
                "daysExcused",
                "daysNotExcused",
                "upToWeek",
                "sy",
                "attPercentage",
                "excusedPercentage",
            ]
        )
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
                    attPercentage = (
                        (daysPresent / daysEnrolled) * 100 if daysEnrolled != 0 else 0
                    )
                    excusedPercentage = (
                        ((daysPresent + daysExcused) / daysEnrolled) * 100
                        if daysEnrolled != 0
                        else 0
                    )
                    cleaned_row = [
                        int(row[0]),  # id
                        row[3],  # name
                        daysEnrolled,  # daysEnrolled
                        float(row[11]),  # daysNotEnrolled
                        daysPresent,  # daysPresent
                        daysExcused,  # daysExcused
                        float(row[15]),  # daysNotExcused
                        week_number,  # week
                        "SY24",  # sy
                        attPercentage,  # attPercentage
                        excusedPercentage,  # excusedPercentage
                    ]
                else:
                    # Check if the 22nd cell is blank
                    if row[21]:
                        # Keep only the specified cells from the row
                        daysEnrolled = float(row[16])
                        daysPresent = float(row[21])
                        daysExcused = float(row[23])
                        attPercentage = (
                            (daysPresent / daysEnrolled) * 100
                            if daysEnrolled != 0
                            else 0
                        )
                        excusedPercentage = (
                            ((daysPresent + daysExcused) / daysEnrolled) * 100
                            if daysEnrolled != 0
                            else 0
                        )
                        cleaned_row = [
                            int(row[0]),
                            row[5],
                            daysEnrolled,
                            float(row[18]),  # daysNotEnrolled
                            daysPresent,
                            daysExcused,
                            float(row[24]),  # daysNotExcused
                            week_number,
                            "SY24",
                            attPercentage,
                            excusedPercentage,
                        ]
                    else:
                        # Keep only the specified cells from the row
                        daysEnrolled = float(row[16])
                        daysPresent = float(row[23])
                        daysExcused = float(row[25])
                        attPercentage = (
                            (daysPresent / daysEnrolled) * 100
                            if daysEnrolled != 0
                            else 0
                        )
                        excusedPercentage = (
                            ((daysPresent + daysExcused) / daysEnrolled) * 100
                            if daysEnrolled != 0
                            else 0
                        )
                        cleaned_row = [
                            int(row[0]),
                            row[5],
                            daysEnrolled,
                            float(row[18]),  # daysNotEnrolled
                            daysPresent,
                            daysExcused,
                            float(row[26]),  # daysNotExcused
                            week_number,
                            "SY24",
                            attPercentage,
                            excusedPercentage,
                        ]
                # Add the cleaned row to the list of cleaned rows
                cleaned_rows.append(cleaned_row)


moveSourceFileToUsedFolder()

# Convert the cleaned rows list to a DataFrame
cleaned_df = pd.DataFrame(cleaned_rows[1:], columns=cleaned_rows[0])
destination_file_path = os.path.join(destination_folder, cleaned_file_name)
# Save the cleaned DataFrame to a new CSV file
cleaned_df.to_csv(destination_file_path, index=False)

# Upload cleaned data to BigQuery table
pandas_gbq.to_gbq(
    cleaned_df,
    f"{project_id}.{dataset_id}.{table_id}",
    project_id=project_id,
    if_exists="append",
)

print("CSV data uploaded to BigQuery table")
