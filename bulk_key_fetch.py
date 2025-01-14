import sys
import os
import subprocess
import json
import csv
from datetime import datetime


def calculate_last_active_timestamp(activity_data):
    # activity = activity_data.get("ACTIVITY", {})
    timestamps = activity_data.get("lastAccessData", {})
    most_recent_timestamp = max(timestamps.values())
    last_active_date = datetime.utcfromtimestamp(most_recent_timestamp).strftime("%m/%d/%Y")
    return last_active_date

def get_data(account_id, keys):
    command = (
        f"sudo AdminTool keys --athenz_private_key_path <path to private key file> "
        f"--athenz_public_certificate_path <path to public certificate file> "
        f"read --keys {','.join(keys)} --deserialize yahoo:{account_id}"
    )
    print(command)
    result = subprocess.check_output(command, shell=True)

    data = json.loads(result)
    deserialized_values = data.get("deserializedValues", {})
    # response = dict()

    deserialized_values_lower = {key.lower(): value for key, value in deserialized_values.items()}

    response = {key: '' for key in keys}

    for key, value in deserialized_values_lower.items():
        if key in keys:
            response[key] = value

    response["last_active_date"] = calculate_last_active_timestamp(deserialized_values.get("ACTIVITY", {}))

    return response

    # key_values = deserialized_values.get("result", {}).get("keyValues", {})
    # last_active_date = calculate_last_active_timestamp(key_values.get("ACTIVITY", {}).get("value", ""))
    # key_values["last_active_date"] = last_active_date
    # return [key_values.get(key, "") for key in keys]

def read_account_ids_from_file(file_path):
    with open(file_path, "r") as file:
        return [line.strip() for line in file]

if len(sys.argv) != 2:
    print("Usage: python script.py <account_ids_file>")
    sys.exit(1)

file_path = sys.argv[1]

input_file_name, file_extension = os.path.splitext(os.path.basename(file_path))
if file_extension == '':
    input_file_name = os.path.basename(file_path)

# keys_to_retrieve = ["abuse_sheriff_notes", "drs", "reginfo", "intl", "abuse_ymf",
#                     "abuse_sid", "abuse_ym", "abuse_status", "yasc_audit_log",
#                     "abuse_comments", "yasc_del_info", "property_usage", "activity",
#                     "cobrand", "state"]

keys_to_retrieve = ["activity", "attr_subscriptions", "ibf"]

account_ids = read_account_ids_from_file(file_path)

output_file_path = f"{input_file_name}.csv"

with open(output_file_path, "w", newline="") as csvfile:
    csv_writer = csv.writer(csvfile)
    header_row = ["accountID"] + keys_to_retrieve
    csv_writer.writerow(header_row)

    for account_id in account_ids:
        try:
            values = get_data(account_id, keys_to_retrieve)
            print(values)
            csv_writer.writerow([account_id] + list(values.values()))
            print(f"Successfully retrieved the data for AccountID: {account_id}")
        except Exception as e:
            print(f"Error retrieving data for AccountID: {account_id}. Error: {str(e)}")

print(f"CSV file with results saved to: {output_file_path}")

