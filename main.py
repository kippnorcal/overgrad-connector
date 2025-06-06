import argparse
import logging
import sys
import traceback
from typing import List
from typing import Union
import re
from datetime import datetime

from job_notifications import create_notifications


from entities.endpoints import create_endpoint_object
from entities.endpoints import Endpoint
from entities.overgrad_api import OvergradAPIPaginator
from entities.overgrad_api import OvergradAPIFetchRecord
from utils.config import OVERGRAD_ENDPOINT_CONFIGS
from utils import helpers
from workflows.process_paginated_records import run_record_processing


logging.basicConfig(
    handlers=[
        logging.FileHandler(filename="app.log", mode="w+"),
        logging.StreamHandler(sys.stdout),
    ],
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %I:%M:%S%p %Z",
)


parser = argparse.ArgumentParser(
    description="Accept start and end date for date window"
)
parser.add_argument(
    "--grad-year",
    help="Required - Filters for a specific grad year; in YYYY format. If not paired with --updated-since or "
         "--recent-updates, all records will be fetched.",
    required=True,
    dest="grad_year",
)
parser.add_argument(
    "--updated-since",
    help="Date to get updates since; YYYY-MM-DD format",
    default=None,
    dest="updated_since",
)
parser.add_argument(
    "--recent-updates",
    help="Gets last updated date from table; Queries api by that date",
    dest="recent_updates",
    action="store_true"
)
args = parser.parse_args()


def validate_date_format(date_string):
    pattern = r'^\d{4}-\d{2}-\d{2}$'
    if not re.match(pattern, date_string):
        raise ValueError("Date must be in YYYY-MM-DD format")
    try:
        # Additional check to ensure the date is valid (e.g., not 2023-02-30)
        datetime.strptime(date_string, "%Y-%m-%d")
    except ValueError:
        raise ValueError("Invalid date")
    return date_string


def _process_university_records(endpoint: Endpoint, api: OvergradAPIFetchRecord, university_id_queue: set) -> None:
    for record in api.fetch_records(university_id_queue):
        cleaned_record = helpers.clean_record_fields(record, endpoint)
        helpers.load_to_cloud_storage(cleaned_record, endpoint)
    logging.info(f"Loaded {len(university_id_queue)} records from {endpoint.name}")


def _setup_endpoints() -> List[Endpoint]:
    """
    Set up and return list of endpoint objects; adds university to the end of the list as it needs to be processed last
    once the university_id_queue is populated
    """
    university_endpoint = None
    endpoints = []
    for endpoint in OVERGRAD_ENDPOINT_CONFIGS:
        if endpoint["name"] != "universities":
            endpoints.append(create_endpoint_object(endpoint))
        else:
            university_endpoint = create_endpoint_object(endpoint)
    if university_endpoint is not None:
        endpoints.append(university_endpoint)
    return endpoints


def main():
    university_id_queue = set()
    endpoints = _setup_endpoints()

    for endpoint in endpoints:
        logging.info(f"Loading data from {endpoint.name}")
        if endpoint.name == "universities":
            if university_id_queue:
                logging.info(f"Loading {len(university_id_queue)} university IDs from queue.")
                api = OvergradAPIFetchRecord(endpoint.name)
                _process_university_records(endpoint, api, university_id_queue)
            else:
                logging.info("No university IDs in queue to load.")
        else:
            if endpoint.has_grad_year:
                date_filter = None
                if endpoint.date_filter:
                    if args.updated_since is not None:
                        try:
                            date_filter = validate_date_format(args.updated_since)
                        except ValueError as e:
                            logging.error(str(e))
                            sys.exit(1)
                    elif args.recent_updates:
                        #TODO: Create func to get date from DW
                        pass
                api = OvergradAPIPaginator(endpoint.name, args.grad_year, date_filter)
            else:
                api = OvergradAPIPaginator(endpoint.name)
            run_record_processing(endpoint, api, university_id_queue, args.grad_year)
            print(f"Length of University Queue is {len(university_id_queue)}")


if __name__ == "__main__":
    notifications = create_notifications("overgrad-connector", "mailgun", logs="app.log")
    try:
        main()
        notifications.notify()
    except Exception as e:
        logging.exception(e)
        stack_trace = traceback.format_exc()
        notifications.notify(error_message=stack_trace)
