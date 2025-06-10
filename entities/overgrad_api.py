from abc import ABC, abstractmethod
import logging
import os
from time import sleep
from typing import Union, Generator

import requests
from tenacity import retry, wait_fixed, retry_if_exception


class OvergradAPIBase(ABC):
    def __init__(self, endpoint):
        self._endpoint = endpoint
        self._session = requests.Session()
        self._api_key = os.getenv("OVERGRAD_API_KEY")
        self._headers = {"ApiKey": self._api_key}
        self._base_url = self._set_base_url()

    @retry(retry=retry_if_exception(requests.exceptions.ConnectionError), wait=wait_fixed(60))
    def _call_endpoint(self, url) -> dict:
        response = self._session.get(url, headers=self._headers, timeout=10)
        response.raise_for_status()
        return response.json()

    @abstractmethod
    def _generate_url(self, *args, **kwargs):
        pass

    @abstractmethod
    def _set_base_url(self):
        pass


class OvergradAPIPaginator(OvergradAPIBase):
    def __init__(self, endpoint, graduation_year: Union[str, None] = None, after_date: Union[str, None] = None):
        self._record_count = 0
        self._total_count = None
        self._total_pages = None
        self._current_page = 1
        self._graduation_year = graduation_year
        self._after_date_str = after_date
        super().__init__(endpoint)

    def _generate_url(self):
        if self._current_page == 1:
            return self._base_url
        else:
            return f"{self._base_url}&page={self._current_page}"

    def _set_base_url(self):
        url = [f"https://api.overgrad.com/api/v1/{self._endpoint}?"]
        if self._graduation_year is not None:
            url.append(f"graduation_year={self._graduation_year}")
        if self._after_date_str is not None:
            url.append(f"updated_after={self._after_date_str}")
        url.append("limit=100")
        return "".join(url[:1]) + "&".join(url[1:])

    @property
    def record_count(self) -> int:
        return self._record_count

    def _is_complete(self) -> bool:
        if self._total_pages is not None:
            return self._current_page > self._total_pages
        else:
            return False

    def _increment_page(self):
        self._current_page += 1

    def _update_response_counts(self, data: dict):
        self._total_count = data["total_count"]
        self._total_pages = data["total_pages"]

    def call_endpoint(self) -> Generator[dict, None, None]:
        while not self._is_complete():
            url = self._generate_url()
            payload = super()._call_endpoint(url)
            data = payload["data"]
            self._record_count += len(data)
            for record in data:
                yield record
            if self._total_count is None:
                self._update_response_counts(payload)
            logging.info(f"Fetched page {self._current_page} of {self._total_pages}")
            self._increment_page()
            sleep(1.1)


class OvergradAPIFetchRecord(OvergradAPIBase):
    def __init__(self, endpoint):
        super().__init__(endpoint)

    def _generate_url(self, record_id: int):
        return f"{self._base_url}/{record_id}"

    def _set_base_url(self):
        return f"https://api.overgrad.com/api/v1/{self._endpoint}"

    def fetch_record(self, record_id) -> dict:
        url = self._generate_url(record_id)
        payload = super()._call_endpoint(url)
        return payload
