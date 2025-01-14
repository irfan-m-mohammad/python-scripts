import json
import os
from glob import glob
from multiprocessing import Pool
from tqdm import tqdm

# Directories
JSON_DIR = "/Users/imohammad/Desktop/new-cloud-trail-logs-json/gdpr-stream-json"
OUTPUT_FILE = "/Users/imohammad/Desktop/new-cloud-trail-logs-json/gdpr-stream-json/assumed-role-arns.txt"

# Function to process a single JSON file
def process_file(file):
    results = set()  # Use a set for deduplication within each file
    try:
        # Load the JSON content
        with open(file, "r") as f:
            data = json.loads(f.read())

        # Search for the assumed role's ARN within the JSON structure
        def find_assumed_role(obj):
            if isinstance(obj, dict):
                # Check if the key is 'arn' and value contains 'assumed-role'
                if "arn" in obj and "assumed-role" in obj["arn"] and "ASC-DELETE-LAMBDA" not in obj["arn"]:
                    # Extract only the role name part of the ARN
                    arn_value = obj["arn"].split("/")[-1]
                    results.add(arn_value)
                # Recursively search in the dictionary
                for key, value in obj.items():
                    find_assumed_role(value)
            elif isinstance(obj, list):
                # Recursively search in the list
                for item in obj:
                    find_assumed_role(item)

        find_assumed_role(data)

    except Exception as e:
        print(f"Error processing {file}: {e}")

    return results

if __name__ == "__main__":
    # Get all JSON files
    json_files = glob(os.path.join(JSON_DIR, "*.json"))

    # Progress bar
    all_results = set()  # Deduplicate results across all files
    with tqdm(total=len(json_files), desc="Processing files") as pbar:
        with Pool() as pool:
            for file_results in pool.imap_unordered(process_file, json_files):
                all_results.update(file_results)  # Update deduplicated results
                pbar.update()

    # Write unique ARNs to the output file
    with open(OUTPUT_FILE, "w") as f:
        f.write("\n".join(sorted(all_results)))

    print(f"Processing complete. Extracted {len(all_results)} unique ARNs saved to {OUTPUT_FILE}.")
