# External libraries
from airflow.models.baseoperator import BaseOperator

from logging import exception as log_exception, info as log_info
from pandas import DataFrame, set_option

set_option('mode.chained_assignment', None)

class CSVReader(BaseOperator):
    def __init__(self, *, file_name: str,  **kwargs) -> None:
        super().__init__(**kwargs)
        self.file_name = file_name

    def execute(self, **kwargs) -> DataFrame:
        self.read_file()
        self.set_data_types()

        return self.main_df

    # Extraction Task
    def read_file(self) -> None:
        from pandas import read_csv
        from os import path, getcwd

        # Create a basic dataframe with the raw data
        # We skip the first 9 rows as these contain the report headers
        log_info('Reading CSV file')
        csv_file_path = path.join(getcwd(), 'dags', self.file_name)
        try:
            df =   read_csv(csv_file_path, skiprows=9)

            # We drop the last row of the df as it contains the report footer
            df = df[:-1]
            self.raw_df = df
        except Exception as e:
            log_exception(e)

    # Set data types
    def set_data_types(self) -> None:
        '''
            This method takes a 'raw' dataframe, and sets the dtypes of all columns, this was determined through a manual analysis

            Args:
                df = Pandas DataFrame, created during the class instantiation
        '''
        log_info('Updating data types')
        try:
            dtypes_dict = {"Customer": "Int32",
                            "Gregorian date": "datetime64[ns]",
                            "Campaign status": "category",
                            "Account status": "category",
                            "Ad group status": "category",
                            "Ad distribution": "category",
                            "Ad status": "category",
                            "Ad type": "category",
                            "Top vs. other": "category",
                            "Device type": "category",
                            "Device OS": "category",
                            "Delivered match type": "category",
                            "BidMatchType": "category",
                            "Language": "category",
                            "Network": "category",
                            "Currency code": "category",
                            "Impressions": "Int16",
                            "Clicks": "Int16",
                            "Conversions": "Int16",
                            "Assists": "Int16",
                            "Spend": "Float64",
                            "Avg. position": "Float64",
                            "Account number": "str",
                            "Account name": "str",
                            "Campaign name": "str",
                            "Ad group": "str",
                            "Ad description": "str",
                            "Ad title": "str",
                            "Tracking Template": "str",
                            "Custom Parameters": "str",
                            "Final Mobile URL": "str",
                            "Final URL": "str",
                            "Display URL": "str",
                            "Final App URL": "str",
                            "Destination URL": "str"}
            
            for key, value in dtypes_dict.items():
                self.raw_df[key] = self.raw_df[key].astype(value)

            # As the below two columns contain data with extra characters, we need to remove those before setting the dtype
            for column in ['Ad group ID', 'Ad ID']:
                self.raw_df[column] = self.raw_df[column].astype('string')
                self.raw_df[column] = self.raw_df[column].str.strip('[]').astype('Int64')
                self.raw_df.replace(['None', 'nan'], '', inplace=True)
            
            self.main_df = self.raw_df
        except Exception as e:
            log_exception(e)