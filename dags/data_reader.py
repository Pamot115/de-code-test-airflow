# External libraries
from airflow.models.dag import DAG

from datetime import datetime as dt
from pandas import set_option

# Internal references
from modules.csv_process import CSVReader
from modules.table_generator import TableGenerator
from modules.db_operator import DbOperator

set_option('mode.chained_assignment', None)

args = {"owner": "Pedro Montes de Oca"}

with DAG(
    dag_id = "table_generator",
    start_date = dt.today(),
    catchup=False,
    tags=["etl_example"],
    default_args = args,
    schedule_interval=None
    ) as dag:
    csv_reader  = CSVReader(task_id = 'read_csv', file_name = 'data/BING_MultiDays.csv')
    table_gen   = TableGenerator(task_id = 'generate_tables', df = csv_reader.execute())
    db_op       = DbOperator(task_id = 'upload_to_db', df_data = table_gen.execute())
        
    csv_reader >> table_gen >> db_op