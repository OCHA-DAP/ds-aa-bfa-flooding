import pandas as pd

from src.utils import db_utils


def load_era5_monthly(adm_level: int = 2):
    engine = db_utils.get_engine("prod")
    query = (
        "SELECT * "
        "FROM public.era5 "
        "WHERE iso3 = 'BFA' "
        f"AND adm_level = {adm_level}"
    )
    df = pd.read_sql(query, engine, parse_dates=["valid_date"])
    df = df.drop(columns=["iso3", "adm_level"])
    df = df.rename(columns={"pcode": f"ADM{adm_level}_PCODE"})
    df = df.sort_values(by=[f"ADM{adm_level}_PCODE", "valid_date"])
    return df
