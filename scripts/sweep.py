"""
Sweep every staging folder and run the loader for anything waiting.

Answers each question ONCE and reuses it, keyed on the exact prompt text: two
scripts asking an identically worded question share the answer, two asking
differently worded questions are asked separately.

total_avg_daily_att and weekly_avg_daily_att are run TOGETHER with the SAME
label, so their prompts are worded identically on purpose and they share one
answer. Both are run twice a week against different windows -- "W02" on the
Monday of W03 for the settled figure, "EOW02" on the Friday of W02 for the
in-progress estimate -- but that is two separate sweeps, not two answers within
one sweep.

Branching Minds is pulled over SFTP first, so anything new is downloaded and
then picked up by the same survey as every other folder. Pass --skip-download to
leave it alone.

Connected Sheets extracts are refreshed LAST, after every load, so the sheets
pick up what was just written rather than what was there before. Pass
--skip-refresh to leave them alone.

Nothing uploads until the plan has been shown and confirmed.
"""

import builtins
import importlib
import os
import sys
import time
import traceback

import colorama
from colorama import Fore, Style
colorama.init(autoreset=True)


def collapseColoramaWrapping():
    """
    Undoes the stacked stdout wrappers left by importing many loaders.

    Every loader calls colorama.init() at module level, and each call captures
    whatever sys.stdout happens to be AT THAT MOMENT as its "original":

        orig_stdout = sys.stdout          # the PREVIOUS wrapper
        sys.stdout  = wrap_stream(orig_stdout, ...)

    So nineteen imports leave nineteen wrappers stacked, and deinit() only
    unwinds the last one -- it restores wrapper eighteen, not the terminal. With
    autoreset each layer turns one write into several, so the cost compounds
    down the stack: measured at 5.7 SECONDS per printed line on a real terminal.

    Restoring sys.__stdout__ -- the stream the interpreter started with -- drops
    the whole stack at once. One init() then puts a single wrapper back.
    """
    sys.stdout = sys.__stdout__
    sys.stderr = sys.__stderr__
    colorama.init(autoreset=True)


class TimedStdout:
    """
    Prefixes every written line with the seconds since the previous write.

    Enabled by --timing. The point is to tell "the script is slow" apart from
    "printing is slow" -- if the gap sits on lines that do no work, the cost is
    in the output path, not the loader.
    """

    def __init__(self, stream):
        self._stream = stream
        self._last = time.time()

    def write(self, text):
        if text.strip():
            now = time.time()
            self._stream.write(f"[{now - self._last:6.2f}s] ")
            self._last = now
        return self._stream.write(text)

    def __getattr__(self, name):
        return getattr(self._stream, name)


scriptName = "Sweep"

SENTINEL = "PUT FILE HERE.txt"

# Files that are never an export.
IGNORED = {SENTINEL, ".DS_Store", "Icon\r", "Icon"}

# Explicit, and ordered. Auto-discovery would sweep up _refactorTest (which
# writes to a `test` dataset that does not exist) and the unmigrated legacy
# scripts, so the registry is a list of things known to be safe to run.
#
# ORDER MATTERS:
#   course_catalog before transcript_audit and grad_requirements -- both read
#   academics.cpsCourseCatalog, so a stale catalog gives stale answers.
#   grad_requirements before grad_req_definition -- the definition publisher
#   reconciles against gradReqProgress and aborts if it is empty.
LOADERS = [
    "course_catalog_update",
    "roster_update",
    "schedules_update",
    "grades_update",
    "total_avg_daily_att_update",
    "weekly_avg_daily_att_update",
    "at_report_update",
    "half_day_report_update",
    "assignment_upload",
    "all_assign_by_student_update",
    "latest_grade_entry_update",
    "FOCUS_update",
    "transcript_audit_update",
    "grad_requirements_update",
    "behavior_incidents_update",
    "behavior_tiers_update",
    "behavior_family_comms_update",
    "behavior_interventions_update",
    "behavior_intervention_notes_update",
    "behavior_progress_monitoring_update",
]

# Publishers have no staging folder -- they build their data from code. Each is
# run only if the loader it depends on ran, since it verifies against that
# loader's output.
PUBLISHERS = {
    "grad_requirements_update": ["grad_req_definition_update"],
}


# Run before the survey so anything it fetches appears in the plan. It is a
# download only -- nothing is written to BigQuery and no source file is consumed
# -- so it runs before the confirmation rather than after.
DOWNLOADER = "branchingMindsDownload"

# Run last. Refreshing before the loads would pull the OLD numbers into the
# sheets, which is worse than not refreshing at all -- it looks current.
REFRESHER = "refresh_sheets"


_answers = {}
_realInput = builtins.input


def cachingInput(prompt=""):
    """
    Asks once per distinct question, then replays the answer.

    Keyed on the exact prompt string, so scripts asking the same question share
    an answer and scripts asking different questions do not. That is what keeps
    the Monday-Friday week and the Friday-Thursday week apart.
    """
    key = str(prompt)
    if key in _answers:
        print(f"{prompt}{_answers[key]}" + Fore.CYAN + "   [reused]")
        return _answers[key]
    value = _realInput(prompt)
    _answers[key] = value
    return value


def pendingFiles(folder):
    """Everything in the folder that is not a sentinel or OS clutter."""
    if not os.path.isdir(folder):
        return None                      # folder missing entirely
    return sorted(
        f for f in os.listdir(folder)
        if f not in IGNORED and not f.startswith(".")
    )


def diagnose():
    """
    Measures print latency before and after collapsing the colorama wrappers.

    Touches nothing -- no folders, no SFTP, no BigQuery. It imports the loaders
    exactly as a real sweep does, which is the thing under suspicion: each one
    calls colorama.init() at module level, and on a terminal that wraps
    sys.stdout, so nineteen imports can leave nineteen wrappers stacked.

    If the BEFORE number is large and the AFTER number is small, stacked
    wrappers were the problem and the fix works. If both are small, printing is
    not the bottleneck and the delay is somewhere else -- run a real sweep with
    --timing to find it.
    """
    print(Style.BRIGHT + "Diagnostic: measuring print latency. Nothing is read or written.\n")

    def timePrints(label, n=40):
        start = time.time()
        for i in range(n):
            print(Fore.GREEN + f"  {label} line {i + 1} of {n}")
        elapsed = time.time() - start
        print(Style.BRIGHT + f"  {label}: {elapsed:.3f}s for {n} lines "
                             f"({elapsed / n * 1000:.1f} ms per line)\n")
        return elapsed

    baseline = timePrints("baseline (1 colorama wrapper)")

    print(Style.BRIGHT + f"Importing {len(LOADERS)} loaders, "
                         "each calling colorama.init()...\n")
    for name in LOADERS:
        try:
            importlib.import_module(name)
        except Exception as exc:
            print(Fore.YELLOW + f"  {name} failed to import: {exc}")

    stacked = timePrints("after imports (wrappers possibly stacked)")

    collapseColoramaWrapping()
    collapsed = timePrints("after collapseColoramaWrapping()")

    print(Style.BRIGHT + "=" * 68)
    print(Style.BRIGHT + f"  baseline           {baseline:7.3f}s")
    print(Style.BRIGHT + f"  after 19 imports   {stacked:7.3f}s")
    print(Style.BRIGHT + f"  after collapse     {collapsed:7.3f}s")
    print()
    # An absolute floor as well as a ratio: on a pipe every number here is
    # ~0.002s, and 0.002 > 0.000 * 3 is true while meaning nothing.
    SLOW = 0.25          # seconds for 40 lines, i.e. >6ms per line
    if stacked < SLOW:
        print(Fore.CYAN + "  Printing is NOT the bottleneck -- 40 lines cost "
                          f"{stacked:.3f}s even with every loader imported.\n"
                          "  Run a real sweep with --timing to see where the seconds go.")
    elif collapsed < stacked / 2:
        print(Fore.GREEN + "  Stacked colorama wrappers WERE the problem, and the "
                           "collapse fixes it.")
    else:
        print(Fore.YELLOW + "  Imports slowed printing and the collapse did not help. "
                            "Something else is wrapping stdout -- worth checking for a "
                            "terminal integration or a shell wrapper.")


def pullBranchingMinds():
    """
    Fetches new Branching Minds exports over SFTP.

    A failure here is reported and does NOT stop the sweep: no credentials, no
    network, or an unknown host key should not prevent a roster that is already
    sitting in its folder from loading. The download skips files it has already
    fetched, so running it on every sweep is cheap.
    """
    print(Style.BRIGHT + f"\n{'=' * 68}\n{DOWNLOADER}\n{'=' * 68}")
    try:
        mod = importlib.import_module(DOWNLOADER)
        mod.doWork()
        return True, None
    except SystemExit as exc:
        # The downloader exits deliberately for missing credentials, an empty
        # remote folder, or a refused host key. All are worth seeing, none are
        # a reason to abandon the rest of the sweep.
        return False, f"stopped itself (exit {exc.code})"
    except Exception as exc:
        traceback.print_exc()
        return False, f"{type(exc).__name__}: {exc}"


def survey():
    """
    Returns (ready, empty, missing) without importing anything twice.

    Importing a loader is safe -- module level is constants and colorama -- but
    it is done once here and the module reused, so a module-level side effect
    cannot happen twice.
    """
    ready, empty, missing = [], [], []

    # Neutralise colorama.init() for the duration of the imports. Each loader
    # calls it at module level, and every call wraps whatever sys.stdout is at
    # that moment -- so importing nineteen of them builds a nineteen-deep stack
    # of wrappers, and with autoreset each layer multiplies the writes below it.
    # Stopping the stack forming is better than unwinding it afterwards.
    realInit = colorama.init
    colorama.init = lambda *a, **k: None
    try:
        ready, empty, missing = _surveyFolders()
    finally:
        colorama.init = realInit
    return ready, empty, missing


def _surveyFolders():
    ready, empty, missing = [], [], []
    for name in LOADERS:
        try:
            mod = importlib.import_module(name)
        except Exception as exc:
            missing.append((name, f"import failed: {type(exc).__name__}: {exc}"))
            continue
        folder = getattr(mod, "source_folder", None)
        if folder is None:
            missing.append((name, "no source_folder"))
            continue
        files = pendingFiles(folder)
        if files is None:
            missing.append((name, f"folder not found: {folder}"))
        elif files:
            ready.append((name, mod, folder, files))
        else:
            empty.append(name)
    return ready, empty, missing


def report(ready, empty, missing):
    print(Style.BRIGHT + f"\n{len(ready)} folder(s) have files waiting.\n")
    for name, _, folder, files in ready:
        print(Fore.GREEN + f"  {name}")
        for f in files:
            print(Fore.GREEN + f"      {f}")
        if len(files) > 1:
            print(Fore.YELLOW + "      more than one file -- findExportFile expects "
                                "exactly one and will refuse this folder.")
    if empty:
        print(Fore.CYAN + f"\n{len(empty)} folder(s) empty: " + ", ".join(empty))
    if missing:
        print(Fore.YELLOW + f"\n{len(missing)} loader(s) could not be checked:")
        for name, why in missing:
            print(Fore.YELLOW + f"      {name}: {why}")


def runOne(name, mod):
    """Runs one loader. A failure is reported and does not stop the sweep."""
    print(Style.BRIGHT + f"\n{'=' * 68}\n{name}\n{'=' * 68}")
    try:
        mod.doWork()
        return True, None
    except SystemExit as exc:
        # Reconciliation failures raise SystemExit(1) on purpose. That is the
        # script refusing to write bad data, not a crash.
        return False, f"stopped itself (exit {exc.code})"
    except Exception as exc:
        traceback.print_exc()
        return False, f"{type(exc).__name__}: {exc}"


def doWork():
    if "--diagnose" in sys.argv:
        diagnose()
        return

    print(Style.BRIGHT + f"Starting {scriptName}...")

    downloadResult = None
    if "--skip-download" in sys.argv:
        print(Fore.CYAN + "Skipping the Branching Minds pull (--skip-download).")
    else:
        ok, why = pullBranchingMinds()
        downloadResult = (DOWNLOADER, ok, why)
        if not ok:
            print(Fore.YELLOW + f"\nBranching Minds pull did not complete -- {why}\n"
                                "Continuing with whatever is already on disk.")

    ready, empty, missing = survey()

    # Importing the loaders ran colorama.init() once per module.
    collapseColoramaWrapping()
    if "--timing" in sys.argv:
        sys.stdout = TimedStdout(sys.stdout)
        print(Fore.CYAN + "Timing on: each line shows seconds since the previous line.")

    report(ready, empty, missing)

    if not ready:
        print(Style.BRIGHT + "\nNothing to upload.")
        return

    print(Fore.CYAN + "\nThese will UPLOAD to BigQuery and then DELETE the source "
                      "file from its folder.")
    if _realInput(Fore.CYAN + "Type 'go' to continue: ").strip().lower() != "go":
        print(Fore.YELLOW + "Aborted. Nothing ran.")
        return

    builtins.input = cachingInput
    results = [downloadResult] if downloadResult else []
    try:
        for name, mod, _, _ in ready:
            ok, why = runOne(name, mod)
            results.append((name, ok, why))

            for pub in PUBLISHERS.get(name, []) if ok else []:
                try:
                    pmod = importlib.import_module(pub)
                    pok, pwhy = runOne(pub, pmod)
                    results.append((pub, pok, pwhy))
                except Exception as exc:
                    results.append((pub, False, f"{type(exc).__name__}: {exc}"))
    finally:
        builtins.input = _realInput

    if "--skip-refresh" in sys.argv:
        print(Fore.CYAN + "\nSkipping the Sheets extract refresh (--skip-refresh).")
    elif not any(ok for _, ok, _ in results):
        print(Fore.CYAN + "\nNothing loaded, so the Sheets extracts are not refreshed.")
    else:
        ok, why = runOne(REFRESHER, importlib.import_module(REFRESHER))
        results.append((REFRESHER, ok, why))

    print(Style.BRIGHT + f"\n{'=' * 68}\nSweep complete.\n")
    for name, ok, why in results:
        print((Fore.GREEN + f"  ok      {name}") if ok
              else (Fore.YELLOW + f"  FAILED  {name}  -- {why}"))

    failed = [n for n, ok, _ in results if not ok]
    if failed:
        print(Fore.YELLOW + f"\n{len(failed)} loader(s) did not complete. Their source "
                            "files are still in place; nothing was archived for them.")

    if _answers:
        print(Fore.CYAN + "\nAnswers used:")
        for prompt, value in _answers.items():
            clean = prompt.replace("\x1b[36m", "").strip()
            print(Fore.CYAN + f"      {clean} {value}")


if __name__ == "__main__":
    doWork()
