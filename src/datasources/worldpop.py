from io import BytesIO

import numpy as np
import rioxarray as rxr

from src.utils import blob_utils

WORLDPOP_BASE_URL = (
    "https://data.worldpop.org/GIS/Population/"
    "Global_2000_2020_1km_UNadj/2020/{iso3_upper}/"
    "{iso3}_ppp_2020_1km_Aggregated_UNadj.tif"
)


def get_blob_name(iso3: str):
    iso3 = iso3.lower()
    return (
        f"{blob_utils.GLB_MONITORING_PROJECT_PREFIX}/raw/worldpop/"
        f"{iso3}_ppp_2020_1km_Aggregated_UNadj.tif"
    )


def load_worldpop_from_blob():
    iso3 = "bfa"
    blob_name = get_blob_name(iso3)
    data = blob_utils.load_blob_data(blob_name, stage="dev")
    da = rxr.open_rasterio(BytesIO(data))
    da = da.where(da != da.attrs["_FillValue"]).squeeze(drop=True)
    da.attrs["_FillValue"] = np.nan
    return da
