@echo off
echo Starting to clean and upload reports for the Panther Post...

:: Change directory to the script's location
cd %~dp0

echo A/T Report
python scripts\at_report_update.py

echo Half Day Report
python scripts\half_day_report_update.py

echo Total Average Daily Attendance Rate Report
python scripts\total_avg_daily_att_update.py

echo Weekly Average Daily Attendance Rate Report
python scripts\weekly_avg_daily_att_update.py

echo Focus Sweep Report
python scripts\focus_sweep_update.py

echo Focus Tardy Report
python scripts\focus_tardy_update.py

:: Pause to see the output
pause
