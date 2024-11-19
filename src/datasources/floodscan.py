import os
from pathlib import Path
from typing import Literal

import xarray as xr

from src.utils import blob_utils

DATA_DIR = Path(os.getenv("AA_DATA_DIR_NEW"))
RAW_FS_HIST_S_PATH = (
    DATA_DIR
    / "private"
    / "raw"
    / "glb"
    / "FloodScan"
    / "SFED"
    / "SFED_historical"
    / "aer_sfed_area_300s_19980112_20231231_v05r01.nc"
)


def get_blob_name(
    data_type: Literal["exposure_raster", "exposure_tabular"],
    date: str = None,
):
    iso3 = "bfa"
    if data_type == "exposure_raster":
        if date is None:
            raise ValueError("date must be provided for exposure data")
        return (
            f"{blob_utils.GLB_MONITORING_PROJECT_PREFIX}/processed/"
            f"flood_exposure/{iso3}/{iso3}_exposure_{date}.tif"
        )
    elif data_type == "exposure_tabular":
        return (
            f"{blob_utils.GLB_MONITORING_PROJECT_PREFIX}/processed/"
            f"flood_exposure/tabular/{iso3}_adm_flood_exposure.parquet"
        )
    else:
        raise ValueError(f"Invalid data_type: {data_type}")


def load_tabular_flood_exposure():
    return blob_utils.load_parquet_from_blob(get_blob_name("exposure_tabular"))


def open_historical_floodscan():
    chunks = {"lat": 100, "lon": 100, "time": 1}
    ds = xr.open_dataset(RAW_FS_HIST_S_PATH, chunks=chunks)
    da = ds["SFED_AREA"]
    da = da.rio.set_spatial_dims(x_dim="lon", y_dim="lat")
    da = da.rio.write_crs(4326)
    return da
