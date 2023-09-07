#!/bin/bash

echo "Starting to clean and upload A/T and Half Day reports"

# Run the first script
python3 scripts/upload-at-report.py

python3 scripts/upload-half-day-report.py

# Pause to see the output
read -p "Press Enter to continue..."
