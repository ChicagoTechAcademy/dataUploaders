#!/bin/bash

# Run the first script
python3 upload-at-report.py

# Run the second script
python3 clean_attendance_csv.py

# Pause to see the output
read -p "Press Enter to continue..."
