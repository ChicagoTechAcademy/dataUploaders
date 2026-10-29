from _dataManager import *
from colorama import init, Fore, Style
init(autoreset=True)


scriptName = "accountBalanceCleaner"


# Constants
project_id = "chitechdb"
dataset_id = "studentInfo"
table_id = "accountBalances"
source_folder = f"../dataUploaders/{table_id}"

schema = [
    {"name": "ENTRY_ID", "type": "INTEGER"},
    {"name": "ID", "type": "INTEGER"},
    {"name": "BALANCE", "type": "FLOAT"},
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

   # keep the statement text (1st column) and the student ID (4th column)
   df = df.iloc[:, [0, 3]].copy()

    # rename the columns
   df.columns = ['TEXT', 'ID']

    # delete all empty rows
   df = df.dropna(how='all')

    # the ID only appears on the "Student" line at the top of each statement; carry it down to the "This is a..." line
   df = df.reset_index(drop=True)
   df['ID'] = df['ID'].ffill()

    # go through each row in the TEXT column. If it's not a string, delete that row.
   df = df[df['TEXT'].apply(lambda x: isinstance(x, str))]

   # go through each row in the TEXT column. If it doesn't start with "T", delete that row.
   df = df[df['TEXT'].str.startswith("This is a", na=False)]

   # Go through the TEXT column. For each row, extract the number after "$" and convert it to a float
   df['BALANCE'] = df['TEXT'].str.extract(r'\$ ([\d,]+\.\d{2})')[0].str.replace(',', '').astype(float)

   # drop the TEXT column
   df = df.drop(columns=['TEXT'])

   # add the entry ID
   df['ENTRY_ID'] = entryID

   # make sure the values in the ID column are integers
   df['ID'] = df['ID'].astype(int)

   return df


def doWork():
    print(Style.BRIGHT + f"Starting {scriptName} script...")

    # Read in the first .csv file found
    csv_file, rawDataFrame = readCSV(source_folder)

    # Clean the data
    cleanedDataFrame = clean_data(rawDataFrame)

    # Replace-all: the delete and the upload are ONE operation.
    deleteAllDataFromTable(project_id, dataset_id, table_id)
    uploadToBigQuery(cleanedDataFrame, schema, project_id, dataset_id, table_id)

    # Archive the source file
    archiveSourceFile(cleanedDataFrame, csv_file, source_folder, table_id)


if __name__ == "__main__":
    doWork()
