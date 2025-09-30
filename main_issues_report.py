import requests
import datetime
import pandas as pd
import yaml
import os
import urllib3

# Disable insecure request warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Constants ---
DOWNLOAD_DIR = os.path.join(os.path.expanduser("~"), "Downloads")
CONFIG_FILE = "config.yaml"

def get_token(base_url, username, password):
    """Generates an authentication token for the Catalyst Center API."""
    url = f"{base_url}/dna/system/api/v1/auth/token"
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    try:
        response = requests.post(url, auth=(username, password), headers=headers, verify=False, timeout=10)
        response.raise_for_status()
        return response.json()["Token"]
    except requests.exceptions.RequestException as e:
        print(f"    ERROR: Could not get token. Details: {e}")
        return None

def get_issues(token, base_url):
    """Retrieves Open issues (default API behavior) from the last 7 days."""
    start_time = int((datetime.datetime.now() - datetime.timedelta(days=7)).timestamp() * 1000)
    # This URL typically fetches open/active issues by default
    url = f"{base_url}/dna/intent/api/v1/issues?startTime={start_time}"
    headers = {"Accept": "application/json", "X-Auth-Token": token}
    try:
        response = requests.get(url, headers=headers, verify=False, timeout=30)
        response.raise_for_status()
        return response.json().get("response", [])
    except requests.exceptions.RequestException as e:
        print(f"    ERROR: Could not get OPEN issues. Details: {e}")
        return []

def get_resolved_issues(token, base_url):
    """Retrieves issues from the last 7 days that have the 'Resolved' status."""
    start_time = int((datetime.datetime.now() - datetime.timedelta(days=7)).timestamp() * 1000)
    # Explicitly filter for Resolved issues
    url = f"{base_url}/dna/intent/api/v1/issues?startTime={start_time}&issueStatus=Resolved"
    headers = {"Accept": "application/json", "X-Auth-Token": token}
    try:
        response = requests.get(url, headers=headers, verify=False, timeout=30)
        response.raise_for_status()
        return response.json().get("response", [])
    except requests.exceptions.RequestException as e:
        print(f"    ERROR: Could not get RESOLVED issues. Details: {e}")
        return []

def get_network_devices(token, base_url):
    """Retrieves all network devices and their roles."""
    url = f"{base_url}/dna/intent/api/v1/network-device"
    headers = {"Accept": "application/json", "X-Auth-Token": token}
    try:
        response = requests.get(url, headers=headers, verify=False, timeout=30)
        response.raise_for_status()
        return response.json().get("response", [])
    except requests.exceptions.RequestException as e:
        print(f"    ERROR: Could not get network devices. Details: {e}")
        return []

def get_device_type(token, device_id, base_url):
    """Retrieves the device type for a given device ID."""
    url = f"{base_url}/dna/intent/api/v1/network-device/{device_id}"
    headers = {"Accept": "application/json", "X-Auth-Token": token}
    try:
        response = requests.get(url, headers=headers, verify=False, timeout=10)
        response.raise_for_status()
        return response.json().get("response", {}).get("type", "Unknown")
    except requests.exceptions.RequestException as e:
        return "Unknown"

def convert_epoch_to_ist(epoch_ms):
    """Converts epoch milliseconds to a readable IST datetime string."""
    
    # 1. Check for NoneType or invalid input
    if not isinstance(epoch_ms, (int, float)) or epoch_ms <= 0:
        return "N/A" # Return a default string instead of causing an error
    
    # Convert milliseconds to seconds
    timestamp_s = epoch_ms / 1000.0
    
    # Create UTC datetime object
    dt_utc = datetime.datetime.fromtimestamp(timestamp_s, tz=datetime.timezone.utc)
    
    # Define IST timezone offset (UTC+5:30)
    ist_offset = datetime.timedelta(hours=5, minutes=30)
    dt_ist = dt_utc + ist_offset
    
    return dt_ist.strftime('%Y-%m-%d %H:%M:%S IST')


def enrich_issues(issues, devices, token, base_url):
    """Enriches a list of issues with device role, hostname, type, and IST timestamp."""
    if not issues:
        return []

    # Create a unified mapping of device ID to device info (role and hostname)
    device_info = {
        dev["id"]: {
            "role": dev.get("role", "Unknown"),
            "hostname": dev.get("hostname", "Hostname_Unknown")
        } 
        for dev in devices
    }

    for issue in issues:
        device_id = issue.get("deviceId")
        
        # --- 1. Device Enrichment ---
        if device_id in device_info:
            info = device_info[device_id]
            issue["deviceHostname"] = info["hostname"] 
            issue["deviceRole"] = info["role"]
            issue["deviceType"] = get_device_type(token, device_id, base_url)
        else:
            issue["deviceHostname"] = "Hostname_Not_Found"
            issue["deviceRole"] = "Unknown"
            issue["deviceType"] = "Unknown"
            
        # --- 2. Timestamp Conversion ---
        # Assuming the relevant timestamp is in the 'timestamp' key (lastOccurredTime is also common)
        timestamp_ms = issue.get("last_occurence_time")
        issue["timestampIST"] = convert_epoch_to_ist(timestamp_ms)
        
    return issues

def create_report(open_issues, resolved_issues, devices, token, base_url, center_name):
    """Creates a uniquely named Excel report for a specific Catalyst Center, including both open and resolved issues."""
    
    all_issues_found = open_issues or resolved_issues
    if not all_issues_found:
        print("    No open or resolved issues found in the last 7 days. No report will be generated for this center.")
        return
    
    print("    Enriching issues with device details and converting timestamp to IST...")
    enriched_open_issues = enrich_issues(open_issues, devices, token, base_url)
    enriched_resolved_issues = enrich_issues(resolved_issues, devices, token, base_url)

    # --- Helper to process and categorize issues with short names (max 31 chars) ---
    def categorize_issues(issues, status_prefix):
        if not issues:
            return {}
        
        # Use only the specified report columns
        df = pd.DataFrame(issues)
        
        # Shortened prefix to comply with openpyxl sheet name length limit
        if status_prefix == "Open Issues":
            prefix = "Open - "
        elif status_prefix == "Resolved Issues":
            prefix = "Resolved - "
        else:
            prefix = ""

        # Mapping device role/type to short sheet names
        device_sheets = {
            f"{prefix}Access Switches": df[(df["deviceRole"].str.upper() == "ACCESS") & (df["deviceType"].str.contains("switch", case=False, na=False))],
            f"{prefix}Access Points": df[(df["deviceRole"].str.upper() == "ACCESS") & (df["deviceType"].str.contains("AP", case=False, na=False))],
            f"{prefix}WLCs": df[(df["deviceRole"].str.upper() == "ACCESS") & (df["deviceType"].str.contains("Controller", case=False, na=False))],
            f"{prefix}Core": df[df["deviceRole"].str.upper() == "CORE"],
            f"{prefix}Distribution": df[df["deviceRole"].str.upper() == "DISTRIBUTION"]
        }
        return device_sheets

    # Categorize both sets of issues
    device_sheets_open = categorize_issues(enriched_open_issues, "Open Issues")
    device_sheets_resolved = categorize_issues(enriched_resolved_issues, "Resolved Issues")

    # Combine all sheets dictionaries
    all_sheets = {**device_sheets_open, **device_sheets_resolved}
    
    # Generate a unique report filename
    report_filename = f"catalyst_center_{center_name}_issues_{datetime.date.today().strftime('%Y%m%d')}.xlsx"
    report_path = os.path.join(DOWNLOAD_DIR, report_filename)
    
    try:
        with pd.ExcelWriter(report_path, engine='openpyxl') as writer:
            for sheet_name, filtered_df in all_sheets.items():
                if not filtered_df.empty:
                    filtered_df.to_excel(writer, sheet_name=sheet_name, index=False)
        print(f"    ✅ Report successfully generated and saved to {report_path}")
    except Exception as e:
        print(f"    ERROR: Could not create Excel report. Details: {e}")

def main():
    """Main function to orchestrate the script execution for all Catalyst Centers."""
    # Load Configuration from YAML file
    try:
        with open(CONFIG_FILE, 'r') as stream:
            config = yaml.safe_load(stream)
            catalyst_centers = config.get('catalyst_centers', [])
            if not isinstance(catalyst_centers, list) or not catalyst_centers:
                print(f"Error: 'catalyst_centers' key is missing, empty, or not a list in {CONFIG_FILE}.")
                return
    except FileNotFoundError:
        print(f"Error: Configuration file '{CONFIG_FILE}' not found. Please create it.")
        return
    except yaml.YAMLError as e:
        print(f"Error parsing YAML file: {e}")
        return

    # Ensure the download directory exists
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)

    # --- Loop through each Catalyst Center defined in the config file ---
    for center in catalyst_centers:
        center_name = center.get('name', 'Unnamed_Center')
        base_url = center.get('url')
        username = center.get('username')
        password = center.get('password')

        print(f"\n--- Processing Catalyst Center: {center_name} ({base_url}) ---")

        if not all([base_url, username, password]):
            print("    ERROR: 'url', 'username', or 'password' is missing for this center. Skipping.")
            continue

        print("1. Generating authentication token...")
        token = get_token(base_url, username, password)
        if not token:
            print(f"    Failed to authenticate with {center_name}. Skipping to next center.")
            continue

        print("2. Retrieving issues from the last 7 days...")
        # Get Open Issues (using original function)
        print("    Fetching Open Issues...")
        open_issues = get_issues(token, base_url)
        
        # Get Resolved Issues (using new dedicated function)
        print("    Fetching Resolved Issues...")
        resolved_issues = get_resolved_issues(token, base_url)

        print("3. Retrieving network device information...")
        devices = get_network_devices(token, base_url)

        print("4. Processing data and creating the Excel report...")
        # Pass both lists to create_report
        create_report(open_issues, resolved_issues, devices, token, base_url, center_name)

    print("\n--- All Catalyst Centers processed. ---")

if __name__ == "__main__":
    main()