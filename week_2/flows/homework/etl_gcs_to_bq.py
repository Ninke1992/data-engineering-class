from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(log_prints=True)
def extract_from_gcs(color: str, year: int, month:int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("data-engineering-bucket")
    gcs_block.get_directory(from_path=gcs_path, local_path="./")
    return Path(f"./{gcs_path}")

# @task()
# def transform(path: Path) -> pd.DataFrame:
#     """Data cleaning example"""
#     df = pd.read_parquet(path)
#     print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
#     df['passenger_count'].fillna(0, inplace = True)
#     print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
#     return df

@task(log_prints=True)
def write_bq(path: Path) -> int:
    """Write DataFrame to BigQuery"""

    gcp_credentials_block = GcpCredentials.load("gcp-creds")
    df = pd.read_parquet(path)
    print(len(df))
    df.to_gbq(
        destination_table= "DEcamp.ny_rides_de",
        project_id= "decamp-375312",
        credentials= gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500000,
        if_exists="append"
    )
    return len(df)

@flow()
def etl_gcs_to_bq(color, year, month) -> None:
    """Main ETL flow to load data into Big Query warehouse"""
    path = extract_from_gcs(color, year, month)
    # df = transform(path)
    nr_rows = write_bq(path)
    return nr_rows

@flow(log_prints=True)
def etl_parent_flow(
    months: list[int] = [2,3], year: int = 2019, color:str = "yellow"):
    total_rows = 0
    for month in months:
        nr_rows = etl_gcs_to_bq(color, year, month)
        total_rows += nr_rows
        print(total_rows)

if __name__ == "__main__":
    color = 'yellow'
    months = [2,3]
    year = 2019
    etl_parent_flow(months, year, color)