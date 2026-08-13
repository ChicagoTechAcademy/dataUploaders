import paramiko
import os
import stat
from datetime import datetime
from dotenv import load_dotenv

# 1. Load sensitive data from the hidden .env file
load_dotenv()

host = os.getenv("SFTP_HOST")
username = os.getenv("SFTP_USER")
password = os.getenv("SFTP_PASS")
remote_base_path = "/exports"  
local_path = "./branchingMindsDownloads" 

# Generate today's date in YYYYMMDD format
current_date = datetime.now().strftime("%Y%m%d")

# Ensure local directory exists
if not os.path.exists(local_path):
    os.makedirs(local_path)

# Verify credentials loaded successfully
if not all([host, username, password]):
    raise ValueError("Missing credentials. Please check your .env file.")

try:
    # 2. Setup SSH Client with Strict Security
    ssh = paramiko.SSHClient()
    ssh.load_system_host_keys()
    
    print(f"Connecting to {host} securely...")
    ssh.connect(hostname=host, port=22, username=username, password=password)
    
    # 3. Initialize SFTP
    sftp = ssh.open_sftp()
    
    # 4. Traverse Subfolders and Download Files
    print(f"Scanning {remote_base_path} for subfolders...")
    
    items = sftp.listdir_attr(remote_base_path)
    
    for item in items:
        # Check if the item is a folder
        if stat.S_ISDIR(item.st_mode):
            folder_name = item.filename
            remote_subfolder = f"{remote_base_path}/{folder_name}"
            
            print(f"\nLooking inside folder: {folder_name}/")
            
            # List the files inside this specific subfolder
            sub_files = sftp.listdir(remote_subfolder)
            
            for file_name in sub_files:
                if file_name.endswith(".csv"):
                    remote_file = f"{remote_subfolder}/{file_name}"
                    
                    # Separate the base name from the ".csv" extension
                    base_name, extension = os.path.splitext(file_name)
                    
                    # Assemble the new name: folderName_fileName_YYYYMMDD.csv
                    safe_local_name = f"{current_date}_{base_name}{extension}"
                    local_file = os.path.join(local_path, safe_local_name)
                    
                    print(f"  Downloading {file_name} as {safe_local_name}...")
                    sftp.get(remote_file, local_file)
            
    print("\nAll downloads complete.")

except paramiko.ssh_exception.SSHException as e:
    print(f"SSH Security/Connection Error: {e}")
    print("Hint: Have you added this server to your known_hosts file?")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
finally:
    # 5. Always close connections
    if 'sftp' in locals():
        sftp.close()
    if 'ssh' in locals():
        ssh.close()