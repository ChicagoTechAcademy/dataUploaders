from _dataManager import *


scriptName = "accountBalanceCleaner"


# Constants
project_id = "chitechdb"
dataset_id = "studentInfo"
table_id = "accountBalances"
source_folder = f"../dataUploaders/{table_id}"

schema = [
    {"name": "entryID", "type": "INTEGER"},
    {"name": "id", "type": "INTEGER"},
    {"name": "balance", "type": "FLOAT"},
]


# Get the entry ID
def getEntryID():

    # find todays date, and reformat it as YYYYMMDD,
    entryID = pd.Timestamp.now().strftime("%Y%m%d")

    print(Fore.CYAN + f"The EntryID is {entryID}.")

    return int(entryID)


# take in a dataframe and extract relevant columns
def clean_data(df):
   
   entryID = getEntryID()

   # delete all columns except the 2nd and 4th
   df = df.iloc[:, [1, 3]].copy()

    # rename the columns
   df.columns = ['TEXT', 'ID']

    # delete all empty rows
   df = df.dropna(how='all')

    # go through each row in the ID column. When you find a number, check if the next row is blank. If so, copy that number. If not, keep the original value.
   df = df.reset_index(drop=True)  # Ensure the index is sequential
   for i in range(len(df) - 1):
       if pd.notna(df.at[i, 'ID']) and pd.isna(df.at[i + 1, 'ID']):
           df.at[i + 1, 'ID'] = df.at[i, 'ID']

    # go through each row in the TEXT column. If it's not a string, delete that row.
   df = df[df['TEXT'].apply(lambda x: isinstance(x, str))]

   # go through each row in the TEXT column. If it doesn't start with "T", delete that row.
   df = df[df['TEXT'].str.startswith("This is a", na=False)]

   # Go through the TEXT column. For each row, extract the number after "$" and convert it to a float
   df['BALANCE'] = df['TEXT'].str.extract(r'\$ ([\d,]+\.\d{2})')[0].astype(float)

   # drop the TEXT column
   df = df.drop(columns=['TEXT'])

   # add the entry ID
   df['ENTRY_ID'] = entryID

   # make sure the values in the ID column are integers
   df['ID'] = df['ID'].astype(int)

   return df


def doWork():
    print(Fore.YELLOW + f"Starting {scriptName} script...")

    # Read in the first .csv file found
    csv_file, rawDataFrame = readCSV(source_folder)

    # Clean the data
    cleanedDataFrame = clean_data(rawDataFrame)

    deleteAllDataFromTable(project_id, dataset_id, table_id)

    # Upload the data to BigQuery
    uploadToBigQuery(cleanedDataFrame, schema, project_id, dataset_id, table_id)

    # Archive the source file
    archiveSourceFile(cleanedDataFrame, csv_file, source_folder, table_id)


doWork()
