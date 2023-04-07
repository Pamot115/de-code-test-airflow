# External libraries
from airflow.models.baseoperator import BaseOperator

from logging import exception as log_exception, info as log_info
from pandas import DataFrame, set_option

set_option('mode.chained_assignment', None)

class TableGenerator(BaseOperator):
    def __init__(self, *, df: DataFrame, **kwargs) -> None:
        super().__init__(**kwargs)
        self.main_df = df

    def execute(self, **context):
        self.gen_dim_tables()
        self.gen_fact_table()

        keys = list(self.__dict__.keys())

        # We obtain a list of all the instantiated dataframes for the process
        dfs = [i for i in keys if i.startswith('df_')]
        df_objects = [getattr(self, i) for i in dfs]

        # Additionally, we generate the table names by removing the dataframe prefix
        df_names = [str(i).replace('df_', '').lower() for i in keys if i.startswith('df_')]

        data_objects = df_objects, df_names
        return data_objects

    # These methods serve to define all dimensional tables based on a manual analysis
    def gen_dim_tables(self):
        '''
            This method servers to define all dimensional tables based on a manual analysis
        '''
        log_info('Generating dimensional tables')
        try:
            self.main_df.replace(['None', 'nan'], '', inplace=True)

            self.df_dim_account = self.main_df[['Account number', 'Account name', 'Account status']].sort_values('Account number').drop_duplicates(ignore_index=True)
            self.df_dim_ad_group = self.main_df[['Ad group ID', 'Ad group', 'Ad group status', 'Language', 'Currency code']].sort_values('Ad group ID').drop_duplicates(ignore_index=True)
            self.df_dim_ads = self.main_df[['Ad ID', 'Ad group ID', 'Ad description', 'Ad distribution', 'Ad status', 'Ad title', 'Ad type']].sort_values('Ad ID').drop_duplicates(ignore_index=True)

            self.df_dim_customer = self.main_df[['Customer', 'Campaign name', 'Campaign status']].sort_values('Campaign name').drop_duplicates(ignore_index=True)
            self.df_dim_customer.insert(0, 'Customer Key', self.df_dim_customer.index)
            self.df_dim_customer['Customer Key'] = self.df_dim_customer['Customer Key'].astype('int16')
            
            self.df_dim_device = self.main_df[['Device type', 'Device OS']].sort_values('Device type').drop_duplicates(ignore_index=True)
            self.df_dim_device.insert(0, 'Device Key', self.df_dim_device.index)
            self.df_dim_device['Device Key'] = self.df_dim_device['Device Key'].astype('int16')

            self.df_dim_network = self.main_df[['Network', 'Top vs. other']].sort_values('Top vs. other').drop_duplicates(ignore_index=True)
            self.df_dim_network.insert(0, 'Network Key', self.df_dim_network.index)
            self.df_dim_network['Network Key'] = self.df_dim_network['Network Key'].astype('int16')

            df_dim_urls = self.main_df[['Display URL', 'Tracking Template', 'Final App URL', 'Final Mobile URL', 'Custom Parameters']]
            df_dim_urls['Navigation URL'] = self.main_df['Destination URL'] + self.main_df['Final URL']
            
            self.df_dim_urls = df_dim_urls.drop_duplicates(ignore_index=True)
            self.df_dim_urls.insert(0, 'URL Key', self.df_dim_urls.index)
            self.df_dim_urls['URL Key'] = self.df_dim_urls['URL Key'].astype('Int16')
            
        except Exception as e:
            log_exception(e)

    def gen_fact_table(self):
        from pandas import merge

        log_info('Generating fact table')
        try:
            # Creating a copy of the original dataframe
            df_tmp_fct = self.main_df

            # Merging these columns 
            df_tmp_fct['Navigation URL'] = self.main_df['Destination URL'] + self.main_df['Final URL']

            # Dropping unnecessary columns
            df_tmp_fct = df_tmp_fct.drop(columns=['Account name', 'Account status', 'Ad group ID', 'Ad group', 'Ad group status', 'Ad description',
                'Ad distribution', 'Ad status', 'Ad title', 'Ad type', 'Language', 'Currency code', 'Final URL',  'Destination URL'])

            # Replacing URL data for a single key
            columns = ['Tracking Template', 'Navigation URL', 'Display URL']
            df_tmp_fct = merge(df_tmp_fct, self.df_dim_urls, how='left', left_on=columns, right_on=columns)
            df_tmp_fct = df_tmp_fct.drop(columns= ['Final Mobile URL_x', 'Final App URL_x', 'Final App URL_y', 'Final Mobile URL_y', 'Custom Parameters_x', 'Custom Parameters_y'])
            df_tmp_fct = df_tmp_fct.drop(columns=columns)

            # Replacing Device data
            columns = ['Device type', 'Device OS']
            df_tmp_fct = merge(df_tmp_fct, self.df_dim_device, how='left', left_on=columns, right_on=columns)
            df_tmp_fct = df_tmp_fct.drop(columns=columns)

            # Replacing Network data
            columns = ['Top vs. other', 'Network']
            df_tmp_fct = merge(df_tmp_fct, self.df_dim_network, how='left', left_on=columns, right_on=columns)
            df_tmp_fct = df_tmp_fct.drop(columns= columns)

            # Replacing Customer data
            columns = ['Customer', 'Campaign name']
            df_tmp_fct = merge(df_tmp_fct, self.df_dim_customer, how='left', left_on=columns, right_on=columns)
            df_tmp_fct = df_tmp_fct.drop(columns= ['Campaign status_x', 'Campaign status_y'])
            df_tmp_fct = df_tmp_fct.drop(columns=columns)

            # Fixing dtypes
            df_tmp_fct['Customer Key'] = df_tmp_fct['Customer Key'].astype('int16')
            df_tmp_fct['Device Key'] = df_tmp_fct['Device Key'].astype('int16')
            df_tmp_fct['Network Key'] = df_tmp_fct['Network Key'].astype('int16')
            df_tmp_fct['URL Key'] = df_tmp_fct['URL Key'].astype('Int16')

            self.df_fct_stats = df_tmp_fct.sort_values('Gregorian date', ignore_index=True)
        except Exception as e:
            log_exception(e)