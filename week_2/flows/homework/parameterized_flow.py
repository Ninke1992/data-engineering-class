from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from prefect.tasks import task_input_hash
from datetime import timedelta
from os import getcwd

@task(retries=3)
def fetch(dataset_url:str) -> pd.DataFrame:
    """Read data from web into pd dataframe"""

    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df:pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime']= pd.to_datetime(df['tpep_dropoff_datetime'])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task(log_prints=True)
def write_local(df:pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write Dataframe out locally as parquet file"""
    path = Path(f"data/{color}/{dataset_file}.csv.gz")
    write_path = f"{dataset_file}.csv.gz"
    df.to_csv(write_path, compression="gzip")
    return write_path, path

@task(log_prints=True)
def write_gcs(write_path:Path, path:Path) -> None:
    """Uploading local parquet file to GCS"""
    gcs_block = GcsBucket.load("data-engineering-bucket")
    gcs_block.upload_from_path(
        from_path = write_path,
        to_path = path
    )
    return

@flow(log_prints=True)
def etl_web_to_gcs(year:int, month:int, color:str):
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean= clean(df)
    write_path, path = write_local(df_clean, color, dataset_file)
    write_gcs(write_path, path)

@flow(log_prints=True)
def etl_parent_flow(
    months: list[int] = [11], year: int = 2020, color:str = "green"):

    for month in months:
        etl_web_to_gcs(year, month, color)

if __name__ == "__main__":
    color = 'yellow'
    months = [11,12]
    year = 2019
    etl_parent_flow(months, year, color)

    