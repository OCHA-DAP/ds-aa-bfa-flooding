from typing import List

import ocha_stratus as stratus
import pandas as pd

from src.utils import db_utils


def load_seas5():
    engine = db_utils.get_engine("prod")
    query = "SELECT * " "FROM public.seas5 " "WHERE iso3 = 'BFA' "
    df = pd.read_sql(query, engine, parse_dates=["valid_date", "issued_date"])
    return df


def load_seas5_pcodes(pcode_list: List[str]):
    query = f"""
    SELECT *
    FROM public.seas5
    WHERE pcode in {tuple(pcode_list)}
    """
    return pd.read_sql(
        query,
        stratus.get_engine("prod"),
        parse_dates=["valid_date", "issued_date"],
    )
