import os
from datetime import datetime
from functools import wraps
from dataclasses import dataclass, field
from typing import List, Literal, Callable, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import joblib
import pandas as pd
from tqdm import tqdm

# from bskb_trading.data.domain.core import DataAsset # blocked by pydantic with "polygon" data_provider

from option_data_research.logger import get_logger
from option_data_research.cfg import GCP_BUCKET_ID, GITIGNORED_CACHE_DIR
from option_data_research.gcs import list_blobs_in_bucket, list_prefixes_in_bucket, load_jsonl_blob, sort_blobs_by_date


logger = get_logger(__file__)
PolygonAssetType = Literal["options", "index", "equity", "forex"]
POLYGON_DATA_TYPE_MAP = {
    "options": "ohlcv",
    "index": "ohlc",
    "equity": "ohlcv",
    "forex": "ohlcv",
}


@dataclass
class PolygonDataAsset:
    asset_type: PolygonAssetType
    asset_id: str

    data_provider: str = "polygon"
    interval_type: str = "1d"
    traded: bool = False

    # data_type is determined by asset_type, which isn't defined here,
    # hence the __post_init__
    data_type: str = field(init=False)

    def __post_init__(self):
        self.data_type = POLYGON_DATA_TYPE_MAP[self.asset_type]

    @property
    def label(self):
        return f"{self.data_provider}.{self.asset_type}.{self.data_type}.{self.interval_type}.{self.asset_id}"

    def __hash__(self):
        return hash(self.label)

    def __eq__(self, other):
        if not isinstance(other, PolygonDataAsset):
            return False

        return other.label == self.label


def list_assets_by_type(asset_type: PolygonAssetType) -> List[PolygonDataAsset]:
    """
    Helper function to list assets in GCS by their type.
    """
    prefix = f"polygon/{asset_type}/{POLYGON_DATA_TYPE_MAP[asset_type]}/1d/"
    all_assets = list_prefixes_in_bucket(GCP_BUCKET_ID, prefix=prefix)

    asset_ids = []
    for asset in all_assets:
        asset = asset.replace(prefix, "")
        if asset.endswith("/"):
            asset = asset[:-1]
        asset_ids.append(asset)

    return [PolygonDataAsset(asset_type=asset_type, asset_id=asset_id) for asset_id in asset_ids]


def list_polygon_options_assets() -> List[PolygonDataAsset]:
    return list_assets_by_type("options")


def list_polygon_index_assets() -> List[PolygonDataAsset]:
    return list_assets_by_type("index")


def list_polygon_equity_assets() -> List[PolygonDataAsset]:
    return list_assets_by_type("equity")


def list_polygon_forex_assets() -> List[PolygonDataAsset]:
    return list_assets_by_type("forex")


CleanerFunc = Callable[[pd.DataFrame, PolygonDataAsset], pd.DataFrame]


def set_sort_dt_index(df: pd.DataFrame, asset: PolygonDataAsset) -> pd.DataFrame:
    # asset doesn't get used here though, but its included as an arg to adhere to CleanerFunc
    df["dt"] = pd.to_datetime(df["dt"], format="mixed")
    df.sort_values("dt", inplace=True)
    df.set_index("dt", inplace=True)
    return df


def add_asset_id_prefixes(df: pd.DataFrame, asset: PolygonDataAsset) -> pd.DataFrame:
    # CleanerFunc
    asset_id = asset.asset_id
    prefixed_columns = [f"{asset_id}_{col}" for col in df.columns]
    df.columns = prefixed_columns
    return df


def ohlc_to_float(df: pd.DataFrame, asset: PolygonDataAsset) -> pd.DataFrame:
    # CleanerFunc
    # TODO: Separate into multiple functions
    # "volume" and "trades" are already ints, so excluded
    ohlcv_cols = ["open", "high", "low", "close"]
    df[ohlcv_cols] = df[ohlcv_cols].astype(float)
    return df


CLEANERS: Dict[PolygonAssetType, List[CleanerFunc]] = {
    "options": [
        set_sort_dt_index,
    ],
    "index": [],  # haven't looked into this data yet
    "equity": [set_sort_dt_index, ohlc_to_float, add_asset_id_prefixes],
    "forex": [
        set_sort_dt_index,
    ],
}


def list_polygon_asset_files(asset: PolygonDataAsset) -> List[str]:
    prefix = asset.label.replace(".", "/")
    if not prefix.endswith("/"):
        # Otherwise prefix = ".../SPY" will also get additional assets' files like ".../SPYD/..."
        prefix += "/"
    blobs = list_blobs_in_bucket(bucket_name=GCP_BUCKET_ID, prefix=prefix)
    return blobs


def log_dataframe_changes(func):
    @wraps(func)
    def wrapper(df: pd.DataFrame, *args, **kwargs):
        before_shape = df.shape
        result_df = func(df, *args, **kwargs)
        after_shape = result_df.shape

        row_change = after_shape[0] - before_shape[0]
        col_change = after_shape[1] - before_shape[1]

        changes = []
        if row_change != 0:
            changes.append(f"{'added' if row_change > 0 else 'removed'} {abs(row_change)} rows")
        if col_change != 0:
            changes.append(f"{'added' if col_change > 0 else 'removed'} {abs(col_change)} columns")

        change_description = " and ".join(changes) if changes else "made no changes"
        logger.info(f"{func.__name__}() {change_description}.")

        return result_df

    return wrapper


def load_polygon_data_asset_from_gcs(
    asset: PolygonDataAsset, blob_limit: Optional[int] = None, clean: bool = True
) -> pd.DataFrame:
    blobs = list_polygon_asset_files(asset=asset)
    blobs = sort_blobs_by_date(blobs)

    if blob_limit:
        blobs = blobs[:blob_limit]

    logger.info(f"Loading {len(blobs)} blobs.")
    data = []

    def load_blob(blob):
        # Convenience function for paralellization below
        return load_jsonl_blob(bucket_name=GCP_BUCKET_ID, blob_name=blob)

    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(load_blob, blob): blob for blob in blobs}
        for future in tqdm(as_completed(futures), total=len(futures)):
            blob_json = future.result()
            data.extend(blob_json)

    df = pd.DataFrame(data)
    assert len(df) > 0, "No data"
    return df


def get_polygon_asset_local_file_name(asset: PolygonDataAsset):
    # Note this does not include the full file path
    return f"{asset.label.replace('.', '/')}.joblib"


class LocalDataCache:
    def __init__(self, data_dir=GITIGNORED_CACHE_DIR):
        self.data_dir = data_dir

    def list_files(self, prefix: str = "") -> List[str]:
        all_files = []
        for dirpath, dirnames, filenames in os.walk(str(self.data_dir)):
            for filename in filenames:
                # Get the relative path from self.data_dir
                relative_path = os.path.relpath(os.path.join(dirpath, filename), self.data_dir)
                all_files.append(relative_path)

        files_with_prefix = [file for file in all_files if file.startswith(prefix)]
        return files_with_prefix

    def load(self, file: str):
        assert isinstance(file, str)
        file_name = self.data_dir / file
        logger.info(f"Loading {file_name}")
        return joblib.load(file_name)

    def save(self, obj, file: str):
        assert isinstance(file, str)
        file_name = self.data_dir / file
        # Ensure the directory exists
        os.makedirs(os.path.dirname(file_name), exist_ok=True)
        logger.info(f"Saving {file_name}")
        return joblib.dump(obj, file_name)

    def get_data_dir_size_gb(self) -> float:
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(self.data_dir):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                total_size += os.path.getsize(fp)
        return total_size / (1024**3)  # Convert bytes to gigabytes


class DataManager(LocalDataCache):
    """
    DataManager class that handles loading and caching data assets.
    Inherits from LocalDataCache to utilize local caching functionality.
    """

    # TODO :This could be a good area to use list_polygon_*_assets() and format the results, handy for notebooks

    def load_asset(self, asset: PolygonDataAsset, force_reload: bool = False, clean: bool = True) -> pd.DataFrame:
        """
        Load the asset either from the local cache or, if not cached or force_reload is True, from the cloud.

        Args:
            asset (PolygonDataAsset): The asset to load.
            force_reload (bool): If True, bypass the cache and reload from the cloud. Default is False.

        Returns:
            pd.DataFrame: The loaded data for the asset.
        """
        cache_file = get_polygon_asset_local_file_name(asset)
        cached_files = self.list_files(prefix=cache_file)

        if not force_reload and cached_files:
            logger.info(f"Loading {asset.label} from local cache.")
            df = self.load(cached_files[0])

        else:
            # If not cached or force_reload is True, load from the cloud
            logger.info(f"Loading {asset.label} from remote.")
            df = load_polygon_data_asset_from_gcs(asset=asset)

            # Cache the data locally
            self.save(df, cache_file)

        if clean:
            for cleaner_func in CLEANERS[asset.asset_type]:
                logged_cleaner_func = log_dataframe_changes(cleaner_func)
                df = logged_cleaner_func(df=df, asset=asset)

        return df
