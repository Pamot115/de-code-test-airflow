# External libraries
from airflow.models.dag import DAG
from datetime import datetime as dt, timedelta

# Internal references
from modules.csv_process import CSVReader
from modules.table_generator import TableGenerator
from modules.db_operator import DbOperator

start_date = dt.now() + timedelta(minutes=5)

args = {
    "owner": "Pedro Montes de Oca",
    "start_date": start_date,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "schedule_interval": '@once'
}

# Creating the DAG Object
with DAG(
    dag_id = "csv_etl",
    tags=["etl_example"],
    default_args = args,
    is_paused_upon_creation = False
    ) as dag:

    # Calling up each part of the process
    csv_reader  = CSVReader(task_id = 'read_csv', file_name = 'data/BING_MultiDays.csv')
    table_gen   = TableGenerator(task_id = 'generate_tables', df = csv_reader.execute())
    db_op       = DbOperator(task_id = 'upload_to_db', df_data = table_gen.execute())

    # Defining the task order    
    csv_reader >> table_gen >> db_op