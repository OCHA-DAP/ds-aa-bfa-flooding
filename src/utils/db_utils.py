import os

from sqlalchemy import create_engine

AZURE_DB_PW_DEV = os.getenv("AZURE_DB_PW_DEV")
AZURE_DB_PW_PROD = os.getenv("AZURE_DB_PW_PROD")


def get_engine(mode):
    if mode == "dev":
        url = (
            f"postgresql+psycopg2://chdadmin:{AZURE_DB_PW_DEV}"
            f"@chd-rasterstats-dev.postgres.database.azure.com/postgres"
        )
    elif mode == "prod":
        url = (
            f"postgresql+psycopg2://chdadmin:{AZURE_DB_PW_PROD}"
            f"@chd-rasterstats-prod.postgres.database.azure.com/postgres"
        )
    else:
        raise ValueError(f"Invalid mode: {mode}")
    return create_engine(url)
