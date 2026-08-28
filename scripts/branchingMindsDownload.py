import os
import stat
import sys
import posixpath
from datetime import datetime

import paramiko
from dotenv import load_dotenv

from colorama import init, Fore, Style
init(autoreset=True)


scriptName = "Branching Minds Download"

# Credentials live in scripts/.env, which is gitignored. Nothing here reads or
# prints a password; the values are handed straight to paramiko.
#
# The path is resolved from this file rather than the working directory. A bare
# load_dotenv() searches upward from the CWD, and the script is invoked from the
# repo root -- so it looked for ./.env, never found scripts/.env, and reported
# every key as missing while the file sat there fully populated.
ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(ENV_PATH)

host = os.getenv("SFTP_HOST")
username = os.getenv("SFTP_USER")
password = os.getenv("SFTP_PASS")

REMOTE_BASE = "/exports"
PORT = 22

# Where an unrouted file lands. Anything not named in REPORT_ROUTES stays here.
STAGING_ROOT = "./branchingMindsDownloads"

# Remote subfolder -> local staging folder.
#
# Each entry sends one report into its own folder so the ordinary loaders can
# consume it with findExportFile(), which expects exactly one CSV per folder.
# Without a route a report lands in STAGING_ROOT alongside everything else, and
# findExportFile() refuses a folder holding several candidates.
#
# EMPTY ON PURPOSE. The remote folder names are not known yet -- nothing has been
# downloaded. Run the script once, read the "unrouted" list it prints, and add
# one line per report:
#
#     "attendance": "./branchingMinds-attendance",
#
REPORT_ROUTES = {
    "new_behavior_incident_report_w_demographics": "./branchingMindsDownloads/behaviorIncidents",
    "tiers_with_demographics_export": "./branchingMindsDownloads/tiers",
    "family_comms_with_demographics_export": "./branchingMindsDownloads/familyComms",
    "intervention_report_with_demographics_export": "./branchingMindsDownloads/interventions",
    "intervention_notes_with_demographics_export": "./branchingMindsDownloads/interventionNotes",
    "behavior_progress_monitoring_plans_and_scores": "./branchingMindsDownloads/progressMonitoring",
    "benchmarks_universal_screeners_w_demographics": "./branchingMindsDownloads/benchmarks",
    # behavior_incident_report_export is the old, empty version of the incident
    # report, superseded by new_behavior_incident_report_w_demographics. It is
    # deliberately unrouted so it lands in STAGING_ROOT and is not loaded.
}


def connect():
    """
    Opens an SFTP session.

    Host keys are loaded from the system known_hosts and no auto-add policy is
    set, so an unrecognised server aborts the connection rather than trusting it.
    That is deliberate: auto-adding would defeat the point of verifying at all.
    """
    ssh = paramiko.SSHClient()
    ssh.load_system_host_keys()

    print(Fore.BLUE + f"Connecting to {host} as {username}...")
    ssh.connect(hostname=host, port=PORT, username=username, password=password)
    return ssh, ssh.open_sftp()


def remoteCSVs(sftp, base):
    """
    Yields (subfolder, filename) for every .csv one level below `base`.

    Only one level down -- that is the layout the export uses. A deeper tree
    would need recursion, and silently ignoring it is better than pretending to
    have walked it.
    """
    for item in sftp.listdir_attr(base):
        if not stat.S_ISDIR(item.st_mode):
            continue
        folder = item.filename
        sub = posixpath.join(base, folder)
        for fileName in sftp.listdir(sub):
            if fileName.lower().endswith(".csv"):
                yield folder, fileName


def localName(folder, fileName, stamp):
    """
    Builds the local filename: YYYYMMDD_folder_file.csv

    The folder name is part of the name on purpose. The previous version dropped
    it and wrote YYYYMMDD_file.csv, so two subfolders holding the same filename
    silently overwrote each other and one report's data vanished with no error.
    """
    base, ext = os.path.splitext(fileName)
    safeFolder = folder.replace(os.sep, "_").replace(" ", "-")
    safeBase = base.replace(os.sep, "_")
    # This export names each file after its own folder, which would produce
    # 20260828_tiers_export_tiers_export.csv. Collapse the repeat.
    if safeBase == safeFolder:
        return f"{stamp}_{safeFolder}{ext}"
    return f"{stamp}_{safeFolder}_{safeBase}{ext}"


def doWork():
    print(Style.BRIGHT + f"Starting {scriptName} script...")

    missing = [k for k, v in
               {"SFTP_HOST": host, "SFTP_USER": username, "SFTP_PASS": password}.items()
               if not v]
    if missing:
        raise SystemExit(
            Fore.YELLOW + f"Missing {', '.join(missing)} in {ENV_PATH}.\n"
            "The file needs SFTP_HOST, SFTP_USER and SFTP_PASS, one per line, "
            "no quotes. It is gitignored."
        )

    stamp = datetime.now().strftime("%Y%m%d")
    os.makedirs(STAGING_ROOT, exist_ok=True)

    ssh = sftp = None
    downloaded, skipped, unrouted = [], [], set()
    try:
        ssh, sftp = connect()

        try:
            sftp.stat(REMOTE_BASE)
        except FileNotFoundError:
            raise SystemExit(
                Fore.YELLOW + f"Remote path '{REMOTE_BASE}' does not exist on {host}."
            )

        print(Fore.BLUE + f"Scanning {REMOTE_BASE} for subfolders...")
        found = list(remoteCSVs(sftp, REMOTE_BASE))

        if not found:
            # An empty result used to print "All downloads complete." and exit 0,
            # which is indistinguishable from a successful pull.
            raise SystemExit(
                Fore.YELLOW + f"No .csv files found under {REMOTE_BASE}. "
                "Nothing downloaded."
            )

        for folder, fileName in found:
            remoteFile = posixpath.join(REMOTE_BASE, folder, fileName)

            destination = REPORT_ROUTES.get(folder)
            if destination is None:
                destination = STAGING_ROOT
                unrouted.add(folder)
            os.makedirs(destination, exist_ok=True)

            target = os.path.join(destination, localName(folder, fileName, stamp))

            if os.path.exists(target):
                # Re-running on the same day would otherwise overwrite a file
                # that may already have been processed.
                print(Fore.YELLOW + f"  {os.path.basename(target)} already exists; skipped.")
                skipped.append(target)
                continue

            print(Fore.GREEN + f"  {folder}/{fileName} -> {os.path.basename(target)}")
            sftp.get(remoteFile, target)

            if os.path.getsize(target) == 0:
                print(Fore.YELLOW + "    downloaded 0 bytes.")
            downloaded.append(target)

    except paramiko.AuthenticationException:
        raise SystemExit(
            Fore.YELLOW + "Authentication failed. Check SFTP_USER and SFTP_PASS in .env."
        )
    except paramiko.SSHException as exc:
        raise SystemExit(
            Fore.YELLOW + f"SSH error: {exc}\n"
            f"If this names an unknown host key, add {host} to known_hosts first:\n"
            f"  ssh-keyscan -p {PORT} {host} >> ~/.ssh/known_hosts\n"
            "Verify the fingerprint with Branching Minds before trusting it."
        )
    finally:
        if sftp:
            sftp.close()
        if ssh:
            ssh.close()

    print(Style.BRIGHT + f"{len(downloaded)} file(s) downloaded"
                         + (f", {len(skipped)} skipped." if skipped else "."))

    if unrouted:
        print(Fore.YELLOW + f"\n{len(unrouted)} report(s) have no route and landed in "
                            f"{STAGING_ROOT}:")
        for folder in sorted(unrouted):
            print(Fore.YELLOW + f'    "{folder}": "{STAGING_ROOT}/{folder}",')
        print(Fore.YELLOW + "Add those lines to REPORT_ROUTES so each report gets its "
                            "own folder. A loader cannot use a folder holding several "
                            "CSVs.")
    print(Style.BRIGHT + f"{scriptName} complete.")


if __name__ == "__main__":
    doWork()
