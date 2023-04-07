# External libraries
from airflow.models.baseoperator import BaseOperator
from logging import exception as log_exception
from airflow.providers.mysql.hooks.mysql import MySqlHook
from pandas import DataFrame

class DbOperator(BaseOperator):
    def __init__(self, *, df_data, **kwargs) -> None:
        super().__init__(**kwargs)
        self.df_objects, self.df_names = df_data

    def execute(self, **context):
        self.export_data()
    
    def connect_db(self):
        # This hook reads the DB connection from the Airflow configuration
        mysql_hook = MySqlHook(
            mysql_conn_id = 'mysql-local-db'
        )
        try:
            # Creating the SQL Alchemy connection engine for pandas
            engine = mysql_hook.get_sqlalchemy_engine()
            return engine
        except Exception as e:
            log_exception(e)
    
    def export_data(self) -> None:
        # We generate the DB connection
        engine = self.connect_db()

        # We export each dataframe to its corresponding MySQL table
        [self.df_objects[index].to_sql(self.df_names[index], con=engine, if_exists='append', index=False) for index, value in enumerate(self.df_names)]