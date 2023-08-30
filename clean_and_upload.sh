#!/bin/bash

# Run the first script
python3 scripts/upload_total_avg_daily_att.py

python3 scripts/upload_weekly_avg_daily_att.py

python3 scripts/upload-at-report.py

python3 scripts/upload-half-day-report.py

# Pause to see the output
read -p "Press Enter to continue..."
