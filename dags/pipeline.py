from airflow.decorators import dag, task
from include.src.google_drive import GoogleDrive
from include.src.postgres_loader import Postgres
from include.src.send_email import Email
from include.src.s3_extract import list_and_process_files
from airflow.operators.python import PythonOperator
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
    schedule_interval='30 10 * * *',
    catchup=False,
    tags=['pipeline_dw_e2e']
)
def dw_e2e():
    drive = GoogleDrive()
    postgres = Postgres(conn_id='postgres_connection')
    email_service = Email(
        conn_id='smtp_connection'
    )

    @task
    def gg_drive_extract_other():
        tables = drive.download_csv_files_from_folder()
        postgres.load_to_postgres(tables)

    @task
    def gg_drive_extract_sales():
        tables = drive.download_csv_files_sales_from_folder()
        postgres.load_to_postgres(tables)
    
    s3_extract = PythonOperator(
        task_id='s3_extract',
        python_callable=list_and_process_files,
        op_kwargs={
            'bucket_name': 'source.dw.e2e',
            'prefix': 'data/'
        },
    )

    transform_dw = DbtTaskGroup(
        group_id='transform_dw',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/dw'],
            test_behavior=TestBehavior.AFTER_ALL
        )
    )

    transform_report = DbtTaskGroup(
        group_id='transform_report',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/report']
        )
    )

    @task
    def send_reports_by_email():
        email_service.extract_and_email_excel(
            query="SELECT * FROM dw_report.report_sale_region;",
            email_list_path='include/email_list.xlsx'
        )

    [gg_drive_extract_other(), gg_drive_extract_sales(), s3_extract] >> transform_dw >> transform_report >> send_reports_by_email()

dw_e2e()