import logging
from typing import List

from entities.endpoints import Endpoint
from entities.overgrad_api import OvergradAPIPaginator
from utils import helpers


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
                        "id": parent_id,
                        "custom_field_id": field_id,
                        "value_type": key,
                        "value": val
                    })
            else:
                flattened.append({
                    "id": parent_id,
                    "custom_field_id": field_id,
                    "value_type": key,
                    "value": value
                })
    return flattened


def _process_custom_fields(record: dict, endpoint: Endpoint, grad_year):
    custom_field_records = _flatten_custom_fields(record, endpoint)
    if custom_field_records:
        filtered_custom_fields = []
        for custom_field in custom_field_records:
            filtered_custom_field = {k: v for k, v in custom_field.items() if k in endpoint.custom_field.fields}
            filtered_custom_fields.append(filtered_custom_field)
        if endpoint.has_grad_year:
            helpers.load_to_cloud_storage(filtered_custom_fields, endpoint.custom_field, grad_year)
        else:
            helpers.load_to_cloud_storage(filtered_custom_fields, endpoint.custom_field)
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


def run_record_processing(endpoint: Endpoint, api: OvergradAPIPaginator, university_id_queue: set, grad_year: str) -> None:
    custom_field_count = 0
    for record in api.call_endpoint():
        if endpoint.has_university_id:
            university_id_queue.add(record.get("university_id"))
        if endpoint.nested_fields is not None:
            _process_nested_fields(record, endpoint)
        if endpoint.custom_field is not None:
            count = _process_custom_fields(record, endpoint)
            if count is not None:
                custom_field_count += count
        cleaned_record = helpers.clean_record_fields(record, endpoint)
        if endpoint.has_grad_year:
            helpers.load_to_cloud_storage(cleaned_record, endpoint, grad_year)
        else:
            helpers.load_to_cloud_storage(cleaned_record, endpoint)
    logging.info(f"Loaded {api.record_count} records from {endpoint.name}")
    if custom_field_count > 0:
        logging.info(f"Loaded {custom_field_count} custom field rows from {endpoint.name}")