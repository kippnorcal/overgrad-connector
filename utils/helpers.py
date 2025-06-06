from io import BytesIO
import json
import os
from typing import Union

from gbq_connector import CloudStorageClient

from entities.endpoints import CustomField
from entities.endpoints import Endpoint

cloud_storage = CloudStorageClient()

def clean_record_fields(record, endpoint):
    """Fill out missing expected fields; Filters out unwanted fields"""
    missing_record_fields = [field for field in endpoint.fields if field not in record.keys()]
    for field in missing_record_fields:
        record[field] = None
    return {k: v for k, v in record.items() if k in endpoint.fields}


def load_to_cloud_storage(
        data: Union[dict, list],
        endpoint: Union[Endpoint, CustomField],
        grad_year: Union[None, str] = None
) -> None:
    record_id = None
    if isinstance(endpoint, CustomField):
        record_id = data[0]["id"]
    if isinstance(endpoint, Endpoint):
        record_id = data["id"]

    if isinstance(data, dict):
        data = [data]

    # Create ndjson file object in memory
    ndjson_lines = [json.dumps(record) for record in data]
    ndjson_content = "\n".join(ndjson_lines).encode('utf-8')
    file_obj = BytesIO(ndjson_content)

    # Load to Google Cloud Storage
    if grad_year is not None:
        blob_name = f"overgrad/{endpoint.gcs_folder}/{grad_year}/{endpoint.file_name_prefix}_{record_id}.ndjson"
    else:
        blob_name = f"overgrad/{endpoint.gcs_folder}/{endpoint.file_name_prefix}_{record_id}.ndjson"
    bucket = os.getenv("BUCKET")
    cloud_storage.load_in_memory_file_to_cloud(bucket, blob_name, file_obj)