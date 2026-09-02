import pandas as pd
from _dataManager import (
    fetchDataFromBigQuery,
    deleteAllDataFromTable,
    uploadToBigQuery,
)
from grad_req_definition import GRAD_REQUIREMENTS, TOTAL_CREDITS

from colorama import init, Fore, Style
init(autoreset=True)


scriptName = "Graduation Requirement Definition"


# Constants
project_id = "chitechdb"
dataset_id = "academics"
table_id = "gradReqDefinition"

tableSchema = [
    {"name": "sortOrder", "type": "INTEGER"},
    {"name": "slot", "type": "STRING"},
    {"name": "cpsBucket", "type": "STRING"},
    {"name": "creditsRequired", "type": "FLOAT"},
]

ORDERED_COLS = [c["name"] for c in tableSchema]

# There is no export behind this table. It is the school's own model, typed out
# in grad_req_definition.py, so this script is a publisher rather than a loader:
# no source folder, no archive step.
#
# RUN ORDER: after grad_requirements_update.py. This script verifies the model
# against the Required figures CPS states in academics.gradReqProgress, so that
# table has to be populated first.


def buildFrame():
    print(Fore.GREEN + f"Building {len(GRAD_REQUIREMENTS)} requirement slots...")
    df = pd.DataFrame(GRAD_REQUIREMENTS, columns=["slot", "cpsBucket", "creditsRequired"])
    df.insert(0, "sortOrder", range(1, len(df) + 1))
    return df[ORDERED_COLS]


def reconcile(df):
    """
    Checks the model against what CPS says it requires.

    The school's slots roll up to CPS's buckets, so each bucket's slot credits
    must sum to the figure CPS states in its own export. A mismatch means either
    CPS changed a requirement or the model here is stale -- both worth stopping
    for, since every downstream "what does this student still need" answer is
    built on these numbers.
    """
    print(Style.BRIGHT + Fore.BLUE + "Reconciling against CPS's stated requirements...")

    total = df["creditsRequired"].sum()
    ok = abs(total - TOTAL_CREDITS) < 0.001
    print((Fore.GREEN if ok else Fore.YELLOW)
          + f"  slots total {total} credits, expected {TOTAL_CREDITS}"
          + ("  MATCH" if ok else "  MISMATCH"))

    try:
        cps = fetchDataFromBigQuery(
            "SELECT requirement, ANY_VALUE(creditsRequired) req "
            "FROM academics.gradReqProgress WHERE requirementKind = 'credit' "
            "AND creditsRequired IS NOT NULL GROUP BY 1")
    except Exception as exc:
        print(Fore.YELLOW + f"  academics.gradReqProgress not readable "
                            f"({type(exc).__name__}); bucket totals not checked.")
        return ok

    stated = dict(zip(cps["requirement"], cps["req"]))
    rolled = df.groupby("cpsBucket")["creditsRequired"].sum().to_dict()

    # RUN ORDER MATTERS: this reconciles against gradReqProgress, so
    # grad_requirements_update.py must have loaded first. An empty table is not
    # a model mismatch and must not be reported as nineteen of them.
    if not stated:
        print(Fore.YELLOW + "  academics.gradReqProgress is EMPTY. Run "
                            "grad_requirements_update.py first, then this script.")
        return False

    for bucket, credits in sorted(rolled.items()):
        cpsValue = stated.get(bucket)
        if cpsValue is None:
            print(Fore.YELLOW + f"  {bucket}: not reported by CPS")
            ok = False
        elif abs(cpsValue - credits) >= 0.001:
            print(Fore.YELLOW + f"  {bucket}: model says {credits}, CPS says {cpsValue}"
                                "  MISMATCH")
            ok = False

    uncovered = sorted(set(stated) - set(rolled))
    if uncovered:
        print(Fore.YELLOW + f"  CPS reports {len(uncovered)} bucket(s) the model does "
                            f"not cover: {uncovered}")

    if ok:
        print(Fore.GREEN + f"  all {len(rolled)} buckets match CPS  MATCH")
    return ok


def doWork():
    """
    Main function for the script.
    """
    print(Style.BRIGHT + f"Starting {scriptName} script...")

    df = buildFrame()

    if not reconcile(df):
        print(Fore.YELLOW + "Aborting: the model does not agree with CPS. Fix "
                            "grad_req_definition.py before publishing.")
        raise SystemExit(1)

    # Replace the table contents. The delete and the upload are ONE operation.
    # ENABLED 2026-09-02. Table created the same day.
    deleteAllDataFromTable(project_id, dataset_id, table_id)
    uploadToBigQuery(df, tableSchema, project_id, dataset_id, table_id)

    print(Style.BRIGHT + f"{scriptName} complete.")


if __name__ == "__main__":
    doWork()
