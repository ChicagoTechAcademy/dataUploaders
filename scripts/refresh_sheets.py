"""
Forces the Connected Sheets extracts to refresh after a load.

Sheets pulls from a BigQuery connector into an extract, and downstream tools
read the extract -- so new warehouse data is invisible until that extract
refreshes. Left alone it refreshes on its own schedule, roughly every 6 hours,
which is long enough to look at a report and see yesterday's numbers.

WHAT IS NEEDED BEFORE THIS WORKS
  1. Enable the Google Sheets API on the chitechdb project.
  2. Share each spreadsheet below with the service account
     (datauploaders@chitechdb.iam.gserviceaccount.com) as an EDITOR. Viewer is
     not enough -- a refresh writes to the sheet.
  3. Add the spreadsheet ids to SPREADSHEETS.

The account keeps BigQuery-only credentials everywhere else; only this module
asks for the spreadsheets scope. See _dataManager.getSheetsCredentials.
"""

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from _dataManager import getSheetsCredentials

from colorama import init, Fore, Style
init(autoreset=True)


scriptName = "Refresh Sheets Extracts"

# (label, spreadsheetId). The id is the long string in the sheet's URL between
# /d/ and /edit.
SPREADSHEETS = [
    # ("Weekly Report", "1AbC...xyz"),
    # ("Panther Post Data", "1DeF...uvw"),
]


def refreshOne(service, label, spreadsheetId):
    """
    Refreshes every data source object in one spreadsheet.

    force=True refreshes regardless of current state. Without it a source that
    Sheets considers already up to date is skipped, which is exactly the case
    here -- the data changed in BigQuery, but Sheets has no way to know that.
    """
    print(Fore.BLUE + f"  {label}...")
    try:
        result = service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheetId,
            body={"requests": [{"refreshDataSource": {"isAll": True, "force": True}}]},
        ).execute()
    except HttpError as exc:
        status = exc.resp.status
        if status == 403:
            print(Fore.YELLOW + f"    403. Share the sheet with the service account "
                                "as an Editor, and check the Sheets API is enabled.")
        elif status == 404:
            print(Fore.YELLOW + "    404. Wrong spreadsheet id, or not shared at all.")
        else:
            print(Fore.YELLOW + f"    HTTP {status}: {exc}")
        return False
    except Exception as exc:
        print(Fore.YELLOW + f"    {type(exc).__name__}: {exc}")
        return False

    # The reply reports per-object state. A refresh is asynchronous: this says
    # it STARTED, not that it finished.
    statuses = []
    for reply in result.get("replies", []):
        for s in reply.get("refreshDataSource", {}).get("statuses", []):
            statuses.append(s.get("refreshResult", {}).get("state", "?"))
    if not statuses:
        print(Fore.YELLOW + "    no data source objects found -- is this sheet "
                            "actually using a BigQuery connector?")
        return False

    print(Fore.GREEN + f"    {len(statuses)} data source object(s): "
                       f"{', '.join(sorted(set(statuses)))}")
    return True


def doWork():
    print(Style.BRIGHT + f"Starting {scriptName}...")

    if not SPREADSHEETS:
        print(Fore.YELLOW + "No spreadsheets configured. Add them to SPREADSHEETS "
                            "in this file; see the notes at the top.")
        return

    service = build("sheets", "v4", credentials=getSheetsCredentials(),
                    cache_discovery=False)

    ok = 0
    for label, spreadsheetId in SPREADSHEETS:
        if refreshOne(service, label, spreadsheetId):
            ok += 1

    print(Style.BRIGHT + f"{ok} of {len(SPREADSHEETS)} spreadsheet(s) refreshed.")
    print(Fore.CYAN + "Refreshes run asynchronously -- the sheet may take a minute "
                      "to finish after this returns.")


if __name__ == "__main__":
    doWork()
