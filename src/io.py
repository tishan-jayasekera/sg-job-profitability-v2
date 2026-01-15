import pandas as pd


def read_excel_sheets(path: str) -> dict:
    return {
        "revenue": pd.read_excel(path, sheet_name="Monthly Revenue"),
        "timesheet": pd.read_excel(path, sheet_name="Timesheet Data"),
        "quote": pd.read_excel(path, sheet_name="Quotation Data"),
    }


def write_parquet(df: pd.DataFrame, path: str) -> None:
    df.to_parquet(path, index=False)


def read_parquet(path: str) -> pd.DataFrame:
    return pd.read_parquet(path)
