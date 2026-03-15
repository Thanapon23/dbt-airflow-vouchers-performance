from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import datetime as dt_module
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import bigquery
import pandas as pd

def upload_to_gsheets():
    # 1. Key Path and spreadsheet id 
    JSON_PATH = '/opt/airflow/gกนแาoogle_key.json' #path
    SPREADSHEET_ID = '1RJ5LWA3hi0MFIMhfRIZONeTQ0qTvjDAPqUdRr3kOKKA' #spreadsheet id
    SHEET_NAME = 'voucher_data' #tab name

    # 2. Google Sheets Connection
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_PATH, scope)
    client = gspread.authorize(creds)
    
    # 3. ingest data from BigQuery
    bq_client = bigquery.Client.from_service_account_json(JSON_PATH)
    query = "SELECT * FROM `ae-project-487716.raw_vouchers.fct_voucher_sales_performance`"
    df = bq_client.query(query).to_dataframe()
    
    # 4. Data Cleaning for spreadsheet -> Converting Timestamp to String
    header = df.columns.values.tolist()
   
    # casting int64/float64 to Python Native types
    raw_data = df.values.tolist() 
    data_rows = []

    for row in raw_data:
        clean_row = []
        for val in row:
            # NA Handling
            if pd.isna(val):
                clean_row.append("")
            # Format date Handling
            elif isinstance(val, (dt_module.date, dt_module.datetime, pd.Timestamp)):
                clean_row.append(str(val))
            else:
                clean_row.append(val)
        data_rows.append(clean_row)
    
    # 5. Write data on sheets (clear and then update)
    sheet = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)
    sheet.clear()
    sheet.update([header] + data_rows, value_input_option='USER_ENTERED')
    print("Successfully uploaded to Google Sheets!")


# Default arguments for all tasks in this DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG (Directed Acyclic Graph)
with DAG(
    'vouchers_transformation_pipeline',
    default_args=default_args,
    description='A pipeline to run dbt models and tests for the voucher project',
    schedule='@daily',
    catchup=False,
    tags=['dbt', 'vouchers'],
) as dag:

# Task 1: Execute dbt run
    run_models = BashOperator(
        task_id='dbt_run',
        #Define the dbt executable path and navigate into the project directory
        bash_command='cd /opt/airflow/dbt && dbt run --profiles-dir .',
    )

    # Task 2: Execute dbt test
    test_models = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/dbt && dbt test --profiles-dir .',
        execution_timeout=timedelta(minutes=5),
    )

    # Task 3: Upload to Google Sheets
    upload_sheets = PythonOperator(
        task_id='upload_to_gsheets',
        python_callable=upload_to_gsheets,
        execution_timeout=timedelta(minutes=5),
    )

    # Set task dependencies: run models first, then perform tests
    run_models >> test_models >> upload_sheets