from airflow.decorators import dag, task
from include.src.google_drive import GoogleDrive
from include.src.postgres_loader import Postgres
import pandas as pd
import os
from datetime import datetime
from airflow.models import Variable
from include.transform_dbt.cosmos_config import DBT_CONFIG, DBT_PROJECT_CONFIG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import ProjectConfig, RenderConfig
from cosmos.constants import TestBehavior


# Define the DAG
@dag(
    start_date=datetime(2024, 8, 17),
    schedule_interval=None,
    catchup=False,
    tags=['data_transfer']
)
def etl():
    drive = GoogleDrive()
    postgres = Postgres(conn_id='postgres_connection')

    def load_to_postgres(tables: dict):
        for table_name, df in tables.items():
            # Create or replace table
            postgres.create_table(table_name, df.columns.to_list())
            # Truncate the table before inserting new data
            postgres.truncate_table(table_name)
            # Insert data into the table
            postgres.insert_data(table_name, df)

    @task
    def extract_load_other():
        tables = drive.download_csv_files_from_folder()
        load_to_postgres(tables)

    @task
    def extract_load_sales():
        tables = drive.download_csv_files_sales_from_folder()
        load_to_postgres(tables)

    transform = DbtTaskGroup(
        group_id='transform',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/dw'],
            test_behavior=TestBehavior.AFTER_ALL
        )
    )

    [extract_load_other(), extract_load_sales()] >> transform

etl()