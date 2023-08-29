import pandas as pd

# Read the CSV file into a DataFrame
input_file = 'daily-attendance-totals.csv'
df = pd.read_csv(input_file, skiprows=3)  # Skip the first 3 rows

# Drop rows with empty 'Code' values
df = df.dropna(subset=['Code'])

# Pivot the DataFrame to reshape the data
df_pivoted = df.pivot(index='YOG 2024', columns='Code', values=['Excused', 'Unexcused'])
df_pivoted.columns = [f'{col[1]}-{col[0]}' for col in df_pivoted.columns]

# Write the modified DataFrame back to the CSV file
output_file = 'modified-daily-attendance.csv'
df_pivoted.to_csv(output_file, index=True, header=True)

print(f"Processing completed and saved to '{output_file}'")
