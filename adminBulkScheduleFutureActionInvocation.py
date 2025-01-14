import csv
import json
import requests
import argparse
import urllib3
import logging
from itertools import islice, chain

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def parse_args():
    parser = argparse.ArgumentParser(description="Bulk API Invocation Script")
    parser.add_argument("--cert", required=True, help="Path to the certificate file")
    parser.add_argument("--key", required=True, help="Path to the private key file")
    parser.add_argument("--api_url", required=True, help="API endpoint URL")
    parser.add_argument("--csv", required=True, help="Path to the CSV input file")
    parser.add_argument("--y-rid", required=True, help="Prefix for the y-rid header")
    parser.add_argument("--batch-size", type=int, default=1000000, help="Batch size for API calls")

    return parser.parse_args()


def chunks(iterable, size):
    iterator = iter(iterable)
    for first in iterator:
        yield chain([first], islice(iterator, size - 1))


def setup_logger(y_rid):
    log_file = f"{y_rid}.log"
    logging.basicConfig(filename=log_file, level=logging.INFO, format="%(asctime)s - %(levelname)s: %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    return logging.getLogger(__name__)


def main():
    args = parse_args()
    logger = setup_logger(args.y_rid)

    with open(args.csv, 'r') as file:
        csv_reader = csv.DictReader(file)

        batch_number = 1

        for batch_chunk in chunks(csv_reader, args.batch_size):
            bulk_request = {"bulkRequest": []}

            logger.info(f"Starting batch {batch_number}")
            for row in batch_chunk:
                schedule_request = {
                    "guid": row['guid'],
                    "yuid": row['yuid'],
                    "namespace": row['namespace'],
                    "drs": row['drs'],
                    "targetDeleteTime": row['targetDeleteTime']
                }

                bulk_request["bulkRequest"].append(schedule_request)
            logger.info(f"Finished reading batch {batch_number} from CSV file")
            logger.info(f"Preparing payload for batch {batch_number}")
            json_data = json.dumps(bulk_request)

            headers = {
                'Content-Type': 'application/json',
                'y-rid': f"{args.y_rid}_{batch_number}"
            }
            logger.info(f"Starting API call for batch {batch_number}")
            response = requests.post(
                args.api_url,
                headers=headers,
                data=json_data,
                cert=(args.cert, args.key),
                verify=False
            )

            logger.info(f"Batch {batch_number} - API Response Status Code: {response.status_code}")
            logger.info(f"Batch {batch_number} - API Response Body: {response.text}")

            if response.status_code == 201:
                logger.info(f"Batch {batch_number} completed successfully.")
            else:
                logger.error(f"Batch {batch_number} failed. Check the response for details.")

            batch_number += 1

    logger.info("All batches processed.")


if __name__ == "__main__":
    main()
