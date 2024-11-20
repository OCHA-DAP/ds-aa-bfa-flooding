from src.utils import blob_utils


def get_blob_name():
    return f"{blob_utils.PROJECT_PREFIX}/processed/cerf/bfa_cerf_flooding.csv"


def load_cerf():
    return blob_utils.load_csv_from_blob(get_blob_name())
