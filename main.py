import argparse
from io import BytesIO
import json
import logging
import os
import sys
import traceback
from typing import List
from typing import Union

from job_notifications import create_notifications
from gbq_connector import CloudStorageClient

from entities.endpoints import create_endpoint_object
from entities.endpoints import Endpoint
from entities.endpoints import CustomField
from entities.overgrad_api import OvergradAPIPaginator
from utils.config import OVERGRAD_ENDPOINT_CONFIGS

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
    "--refresh-dbt",
    help="Refreshes dbt models downstream",
    dest="dbt_refresh",
    action="store_true"
)


def _load_to_cloud_storage(data: Union[dict, list], endpoint: Union[Endpoint, CustomField], cloud_storage: CloudStorageClient) -> None:
    record_id = None
    if isinstance(endpoint, CustomField):
        record_id = data[0][endpoint.parent_id_key]
    if isinstance(endpoint, Endpoint):
        record_id = data["id"]

    if isinstance(data, dict):
        data = [data]

    # Create ndjson file object in memory
    ndjson_lines = [json.dumps(record) for record in data]
    ndjson_content = "\n".join(ndjson_lines).encode('utf-8')
    file_obj = BytesIO(ndjson_content)

    # Load to Google Cloud Storage
    blob_name = f"overgrad/{endpoint.gcs_folder}/{endpoint.file_name_prefix}_{record_id}.ndjson"
    bucket = os.getenv("BUCKET")
    cloud_storage.load_in_memory_file_to_cloud(bucket, blob_name, file_obj)


def _flatten_custom_fields(record: dict, endpoint: Endpoint) -> List[dict]:
    custom_fields = record.pop(endpoint.custom_field.field_name, [])
    parent_id = record.get("id")
    flattened = []

    for item in custom_fields:
        field_id = item["custom_field_id"]
        for key, value in item.items():
            if key == "custom_field_id":
                continue # Do nothing with this

            if key == "multiselect":
                for val in value:
                    flattened.append({
                        endpoint.custom_field.parent_id_key: parent_id,
                        "custom_field_id": field_id,
                        "value_type": key,
                        "value": val
                    })
            else:
                flattened.append({
                    endpoint.custom_field.parent_id_key: parent_id,
                    "custom_field_id": field_id,
                    "value_type": key,
                    "value": value
                })
    return flattened

def _process_custom_fields(record: dict, endpoint: Endpoint, cloud_storage: CloudStorageClient):
    custom_field_records = _flatten_custom_fields(record, endpoint)
    if custom_field_records:
        filtered_custom_fields = []
        for custom_field in custom_field_records:
            filtered_custom_field = {k: v for k, v in custom_field.items() if k in endpoint.custom_field.fields}
            filtered_custom_fields.append(filtered_custom_field)
        _load_to_cloud_storage(filtered_custom_fields, endpoint.custom_field, cloud_storage)
        return len(filtered_custom_fields)

def _flatten_nested_fields(parent_field_name: str, child_fields: dict) -> dict:
    flattened_dict = {}
    for field, value in child_fields.items():
        flattened_name = f"{parent_field_name}_{field}"
        flattened_dict[flattened_name] = value
    return flattened_dict


def _process_nested_fields(record: dict, endpoint: Endpoint) -> None:
    for nested_field in endpoint.nested_fields:
        child_fields = record.pop(nested_field)
        if child_fields:
            flattened_fields = _flatten_nested_fields(nested_field, child_fields)
            record.update(flattened_fields)


def _process_paginated_data(endpoint: Endpoint, api: OvergradAPIPaginator, university_id_queue: set, cloud_storage: CloudStorageClient) -> None:
    custom_field_count = 0
    for record in api.call_endpoint():
        if endpoint.has_university_id:
            university_id_queue.add(record.get("university_id"))
        if endpoint.nested_fields is not None:
            _process_nested_fields(record, endpoint)
        if endpoint.custom_field is not None:
            count = _process_custom_fields(record, endpoint)
            custom_field_count += count

        missing_record_fields = [k for k, v in record.items() if k not in endpoint.fields]
        for field in missing_record_fields:
            record[field] = None
        filtered_record = {k: v for k, v in record.items() if k in endpoint.fields}
        _load_to_cloud_storage(filtered_record, endpoint, cloud_storage)
    logging.info(f"Loaded {api.record_count} records from {endpoint.name}")
    if custom_field_count > 0:
        logging.info(f"Loaded {custom_field_count} custom field rows from {endpoint.name}")

def main():
    university_id_queue = set()
    e = ["students"]
    endpoints = [create_endpoint_object(endpoint) for endpoint in OVERGRAD_ENDPOINT_CONFIGS if endpoint["name"] in e]
    cloud_storage = CloudStorageClient()

    for endpoint in endpoints:
        print(f"Loading data from {endpoint.name}")
        if endpoint.name == "universities":
            pass
        else:
            api = OvergradAPIPaginator(endpoint.name)
            _process_paginated_data(endpoint, api, university_id_queue, cloud_storage)


if __name__ == "__main__":
    notifications = create_notifications("overgrad-connector", "mailgun", logs="app.log")
    try:
        main()
        notifications.notify()
    except Exception as e:
        logging.exception(e)
        stack_trace = traceback.format_exc()
        notifications.notify(error_message=stack_trace)