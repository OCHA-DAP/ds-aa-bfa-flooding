import requests

from src.utils import blob_utils

FIELDMAPS_BASE_URL = "https://data.fieldmaps.io/cod/originals/{iso3}.shp.zip"


def get_blob_name(iso3: str = "bfa"):
    iso3 = iso3.lower()
    return f"{blob_utils.PROJECT_PREFIX}/raw/codab/{iso3}.shp.zip"


def download_codab_to_blob(clobber: bool = False):
    iso3 = "bfa"
    blob_name = get_blob_name(iso3=iso3)
    if not clobber and blob_name in blob_utils.list_container_blobs(
        name_starts_with=f"{blob_utils.PROJECT_PREFIX}/raw/codab/"
    ):
        print(f"{blob_name} already exists in blob storage")
        return
    url = FIELDMAPS_BASE_URL.format(iso3=iso3)
    response = requests.get(url)
    response.raise_for_status()
    blob_utils.upload_blob_data(blob_name, response.content, stage="dev")


def load_codab_from_blob(admin_level: int = 2):
    iso3 = "bfa"
    shapefile = f"{iso3}_adm{admin_level}.shp"
    gdf = blob_utils.load_gdf_from_blob(
        blob_name=get_blob_name(iso3),
        shapefile=shapefile,
        stage="dev",
    )
    return gdf
