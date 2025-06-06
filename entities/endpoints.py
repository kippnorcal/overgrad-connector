from dataclasses import dataclass
from typing import Union


@dataclass
class CustomField:
    field_name: str
    gcs_folder: str
    file_name_prefix: str
    fields: set


@dataclass
class Endpoint:
    name: str
    gcs_folder: str
    file_name_prefix: str
    fields: set
    has_university_id: bool
    date_filter: bool
    has_grad_year: bool
    nested_fields: Union[None,list] = None
    custom_field: Union[None, CustomField] = None


def create_endpoint_object(config: dict) -> Endpoint:
    endpoint = Endpoint(
        name=config["name"],
        gcs_folder=config["gcs_folder"],
        file_name_prefix=config["file_name_prefix"],
        fields=config["fields"],
        has_university_id=config["has_university_id"],
        date_filter=config["date_filter"],
        has_grad_year=config["has_grad_year"]
    )
    endpoint.nested_fields = config.get("nested_fields")
    if config.get("custom_field"):
        custom_field = _create_custom_field_object(config["custom_field"])
        endpoint.custom_field = custom_field

    return endpoint


def _create_custom_field_object(field: dict) -> CustomField:
    return CustomField(**field)
