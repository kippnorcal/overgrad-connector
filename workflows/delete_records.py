import logging
import os
from typing import Union

from entities.endpoints import Endpoint
from entities.overgrad_api import OvergradAPIPaginator

from gbq_connector import BigQueryClient
from gbq_connector import CloudStorageClient
from google.api_core.exceptions import NotFound

gbq = BigQueryClient()
cloud_storage = CloudStorageClient()

bucket = os.getenv("BUCKET")
dataset = os.getenv("GBQ_DATASET")
project = os.getenv("GBQ_PROJECT")


def _delete_record(endpoint: Endpoint, record_id: str, year: str):
    record_path = f"overgrad/{endpoint.gcs_folder}/{year}/{endpoint.file_name_prefix}_{record_id}.ndjson"
    cloud_storage.delete_file(bucket, record_path)
    custom_fields = endpoint.custom_field
    if custom_fields is not None:
        try:
            custom_field_record_path = f"overgrad/{custom_fields.gcs_folder}/{year}/{custom_fields.file_name_prefix}_{record_id}.ndjson"
            cloud_storage.delete_file(bucket, custom_field_record_path)
            logging.info(f"Deleted {endpoint.name} record and custom fields")
        except NotFound:
            # If there are no custom field records, do nothing
            logging.info(f"Deleted {endpoint.name} record; No custom fields were found")
    else:
        logging.info(f"Deleted {endpoint.name} record; No custom fields were found")


def _get_dw_student_ids(year: str) -> Union[set, None]:
    query = f"SELECT overgrad_student_id FROM `{project}.{dataset}.stg_og__students` where graduation_year = {year}"
    df = gbq.query(query)
    if df is not None:
        return set(str(x) for x in df["overgrad_student_id"].to_list())
    else:
        return None


def _get_dw_non_student_ids(endpoint: Endpoint, grad_year: str) -> Union[set, None]:
    id_field = "overgrad_application_id" if endpoint.name == "admissions" else "overgrad_following_id"
    query = f"""
        SELECT {id_field} FROM `{project}.{dataset}.stg_og__{endpoint.name}` as data
        LEFT JOIN `{project}.{dataset}.stg_og__students` as students
        on data.overgrad_student_id = students.overgrad_student_id
        where students.graduation_year = {grad_year}
        """
    df = gbq.query(query)
    if df is not None:
        return set(str(x) for x in df[id_field].to_list())
    else:
        return None


def run_delete_records_workflow(api: OvergradAPIPaginator, endpoint: Endpoint, grad_year: str) -> None:

    logging.info(f"Running deletion workflow for {endpoint.name}")

    ids_from_dw = None
    if endpoint.name == "students":
        ids_from_dw = _get_dw_student_ids(grad_year)
    else:
        ids_from_dw = _get_dw_non_student_ids(endpoint, grad_year)

    if ids_from_dw is not None:
        ids_from_api = set()
        for record in api.call_endpoint():
            ids_from_api.add(str(record["id"]))

        missing_ids = list(ids_from_dw - ids_from_api)

        if missing_ids:
            logging.info(f"Found {len(missing_ids)} record(s) to delete")
            for record in missing_ids:
                logging.info(f"Deleting {record}")
                _delete_record(endpoint, record, grad_year)
        else:
            logging.info("No records to delete")
