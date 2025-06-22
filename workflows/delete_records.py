import logging
import os
from typing import Union

from entities.record_delete_dataclass import DeleteRecord
from entities.overgrad_api import OvergradAPIPaginator

from gbq_connector import BigQueryClient
from gbq_connector import CloudStorageClient
from google.api_core.exceptions import NotFound

gbq = BigQueryClient()
cloud_storage = CloudStorageClient()

bucket = os.getenv("BUCKET")
dataset = os.getenv("GBQ_DATASET")
project = os.getenv("GBQ_PROJECT")


def _delete_admissions_records(record: DeleteRecord, year):
    if record.admission_ids:
        for r in record.admission_ids:
            admission_record_path = f"overgrad/admissions/{year}/admission__{r}.ndjson"
            cloud_storage.delete_file(bucket, admission_record_path)
            try:
                admission_custom_field_record_path = f"overgrad/admissions_custom_fields/{year}/admissions_custom_fields__{r}.ndjson"
                cloud_storage.delete_file(bucket, admission_custom_field_record_path)
            except NotFound:
                # If there are no custom field records, do nothing
                pass
        logging.info(f"Deleted {len(record.admission_ids)} admission record(s) and their custom fields")
    else:
        logging.info(f"No admissions records to delete")

def _delete_followings_records(record: DeleteRecord, year):
    if record.following_ids:
        for r in record.following_ids:
            followings_record_path = f"overgrad/followings/{year}/following__{r}.ndjson"
            cloud_storage.delete_file(bucket, followings_record_path)
        logging.info(f"Deleted {len(record.following_ids)} followings record(s)")
    else:
        logging.info(f"No followings records to delete")


def _delete_student_records(record: DeleteRecord, year):
    student_record_path = f"overgrad/students/{year}/student__{record.student_id}.ndjson"
    cloud_storage.delete_file(bucket, student_record_path)
    try:
        student_custom_field_record_path = f"overgrad/students_custom_fields/{year}/students_custom_fields__{record.student_id}.ndjson"
        cloud_storage.delete_file(bucket, student_custom_field_record_path)
        logging.info(f"Deleted student record and custom fields")
    except NotFound:
        # If there are no custom field records, do nothing
        logging.info(f"Deleted student record; No custom fields were found")


def _get_dw_student_ids(year: str) -> Union[set, None]:
    query = f"SELECT overgrad_student_id FROM `{project}.{dataset}.stg_og__students` where graduation_year = {year}"
    df = gbq.query(query)
    if df is not None:
        return set(str(x) for x in df["overgrad_student_id"].to_list())
    else:
        return None

# admissions
# SELECT * FROM `kae-cloud-kippnorcal.norcal_analytics.stg_og__admissions` where overgrad_student_id = 999999

def _find_student_admissions(record: DeleteRecord):
    # SELECT * FROM `kae-cloud-kippnorcal.norcal_analytics.stg_og__followings` where overgrad_student_id = 999999
    query = f"SELECT overgrad_application_id FROM `{project}.{dataset}.stg_og__admissions` where overgrad_student_id = {record.student_id}"
    df = gbq.query(query)
    if df is not None:
        result = df["overgrad_application_id"].to_list()
        record.admission_ids = result

def _find_student_followings(record: DeleteRecord):
    # SELECT * FROM `kae-cloud-kippnorcal.norcal_analytics.stg_og__followings` where overgrad_student_id = 999999
    query = f"SELECT overgrad_following_id FROM `{project}.{dataset}.stg_og__followings` where overgrad_student_id = {record.student_id}"
    df = gbq.query(query)
    if df is not None:
        result = df["overgrad_following_id"].to_list()
        record.following_ids = result

def run_delete_records_workflow(api: OvergradAPIPaginator, grad_year: str) -> None:
    # get records from DW
    dw_student_ids = _get_dw_student_ids(grad_year)
    if dw_student_ids is not None:
        api_student_ids = set()
        for record in api.call_endpoint():
            api_student_ids.add(str(record["id"]))

        missing_student_ids = list(dw_student_ids - api_student_ids)
        delete_records_obj = []

        if missing_student_ids:
            logging.info(f"Found {len(missing_student_ids)} record(s) to delete")
            for sid in missing_student_ids:
                delete_records_obj.append(DeleteRecord(student_id=sid))

        if delete_records_obj:
            for record in delete_records_obj:
                _find_student_admissions(record)
                _find_student_followings(record)
                logging.info(f"Deleting student ID {record.student_id} and its associated records")
                _delete_student_records(record, grad_year)
                _delete_admissions_records(record, grad_year)
                _delete_followings_records(record, grad_year)
        else:
            logging.info("No records to delete")
    else:
        logging.info(f"No records for grad year {grad_year}")