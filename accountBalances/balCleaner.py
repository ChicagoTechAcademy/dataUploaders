import csv

# Open the CSV file and extract relevant data
def extract_data(input_file, output_file):
    with open(input_file, 'r', encoding='utf-8') as csvfile:
        reader = list(csv.reader(csvfile))
        extracted_data = []
        
        for i, row in enumerate(reader):
            print(f"Processing row {i + 1}: {row}")  # Debugging: print the row to understand its structure

            # Scan for "To the parent/guardian of:" in the 2nd column
            if len(row) > 1 and "To the parent/guardian of:" in row[1]:
                if i + 1 < len(reader):
                    next_row = reader[i + 1]
                    name = next_row[2] if len(next_row) > 2 else None
                    print(f"Found name in row {i + 2}: {name}")  # Debugging: print the found name
                    
            # Scan for balance in the 2nd column
            elif len(row) > 1 and row[1].startswith("This is a current statement of your account.  The total amount due is  "):
                balance = row[1][len("This is a current statement of your account.  The total amount due is  "):].strip()
                print(f"Found balance in row {i + 1}: {balance}")  # Debugging: print the found balance
                
                if name and balance:
                    extracted_data.append([name, balance])
                    print(f"Extracted data - Name: {name}, Balance: {balance}")  # Debugging: print the extracted data
                    name = None

    # Write the extracted data into a new CSV file
    if extracted_data:
        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['NAME', 'BALANCE'])
            writer.writerows(extracted_data)
    else:
        print("No data extracted.")

# Set input and output file paths
input_file = 'rawbal.csv'
output_file = 'extracted_name_balance.csv'

# Extract data from CSV and save it to a new CSV file
extract_data(input_file, output_file)
print(f"Data extracted and saved to {output_file}")
