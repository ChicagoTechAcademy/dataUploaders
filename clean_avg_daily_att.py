import csv
import os

file_name = "avg-daily-att.csv"

# Open the CSV file for reading
with open(file_name, 'r') as csv_file:
  # Create a CSV reader
  reader = csv.reader(csv_file)
  # Create a list to store the cleaned rows
  cleaned_rows = []
  # Add the header row to the list of cleaned rows
  cleaned_rows.append(["ID", "Name", "Days Enrolled", "Days Not Enrolled", "Days Present", "Days Excused", "Days Not Excused"])
  # Iterate through all rows in the CSV file
  for row in reader:
    # Check if the first cell in the row is a number
    if row and row[0].isdigit():
      # Check if the 4th cell is blank
      if row[3]:
        # Keep only the specified cells from the row
        cleaned_row = [row[0], row[3], row[9], row[11], row[12], row[14], row[15]]
      else:
        # Check if the 22nd cell is blank
        if row[21]:
          # Keep only the specified cells from the row
          cleaned_row = [row[0], row[5], row[16], row[18], row[21], row[23], row[24]]
        else:
          # Keep only the specified cells from the row
          cleaned_row = [row[0], row[5], row[16], row[18], row[23], row[25], row[26]]
      # Add the cleaned row to the list of cleaned rows
      cleaned_rows.append(cleaned_row)
# Open the CSV file for writing
with open(file_name, 'w', newline='') as csv_file:
  # Create a CSV writer
  writer = csv.writer(csv_file)
  # Write the cleaned rows to the CSV file
  writer.writerows(cleaned_rows)
