import pandas as pd

from src.utils import db_utils


def load_seas5():
    engine = db_utils.get_engine("prod")
    query = "SELECT * " "FROM public.seas5 " "WHERE iso3 = 'BFA' "
    df = pd.read_sql(query, engine)
    return df
