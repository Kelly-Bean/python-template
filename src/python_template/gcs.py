import json
from typing import Optional, List
from functools import lru_cache
from datetime import datetime

from google.cloud import storage

from option_data_research.logger import get_logger
from option_data_research.cfg import GCP_PROJECT_ID


LOG = get_logger(__file__)


@lru_cache(1)
def get_gcs_client():
    return storage.Client(project=GCP_PROJECT_ID)


def get_gcs_blob(name, bucket):
    bucket = get_gcs_client().bucket(bucket)
    blob = bucket.blob(name)
    return blob


def load_jsonl_blob(bucket_name, blob_name):
    client = get_gcs_client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    content = blob.download_as_text()

    json_objects = []
    for line in content.splitlines():
        if line.strip():  # Skip any empty lines
            json_objects.append(json.loads(line))

    return json_objects


def list_blobs_in_bucket(bucket_name, prefix: Optional[str] = None, limit: Optional[int] = None):
    client = get_gcs_client()
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix, max_results=limit)
    blob_list = []
    for blob in blobs:
        blob_list.append(blob.name)
    return blob_list


def list_buckets():
    client = get_gcs_client()
    buckets = client.list_buckets()
    bucket_list = []
    for bucket in buckets:
        bucket_list.append(bucket.name)
    return bucket_list


def list_prefixes_in_bucket(bucket_name, prefix=None, delimiter="/"):
    """
    eg.
    > list_prefixes_in_bucket(GCP_BUCKET_ID)
    > list_prefixes_in_bucket(GCP_BUCKET_ID, prefix="polygon/options/ohlcv/1d/")
    """
    client = get_gcs_client()
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix, delimiter=delimiter)
    prefixes = set()
    for page in blobs.pages:
        prefixes.update(page.prefixes)
    return list(prefixes)


def sort_blobs_by_date(blobs: List[str]) -> List[str]:
    """
    Sort a list of blobs by the datetime in their filenames.
    """

    def extract_date(blob: str) -> datetime:
        # Assumes the date is always in the format 'YYYY-MM-DD' before the .jsonl extension
        date_str = blob.split("/")[-1].replace(".jsonl", "")
        return datetime.strptime(date_str, "%Y-%m-%d")

    return sorted(blobs, key=extract_date)
