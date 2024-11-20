from src.utils import blob_utils


def load_emdat():
    blob_name = (
        f"{blob_utils.PROJECT_PREFIX}/raw/emdat/"
        f"emdat_bfa_floods_accessed_2024-11-19.xlsx"
    )
    return blob_utils.load_excel_from_blob(blob_name)
