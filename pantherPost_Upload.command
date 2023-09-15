#!/bin/bash

echo "Starting to clean and upload reports for the Panther Post..."

# Change directory to the script's location
cd "$(dirname "$0")"

echo "A/T Report"
# Run the first script
python3 scripts/at_report_update.py

echo "Half Day Report"
python3 scripts/half_day_report_update.py

echo "Total Average Daily Attendance Rate Report"
python3 scripts/total_avg_daily_att_update.py

echo "Weekly Average Daily Attendance Rate Report"
python3 scripts/weekly_avg_daily_att_update.py

echo "Focus Sweep Report"
python3 scripts/focus_sweep_update.py

echo "Focus Tardy Report"
python3 scripts/focus_tardy_update.py

# Pause to see the output
read -p "Press Enter to continue..."
