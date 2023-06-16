class DatabaseConnector():

    def __init__(self,db_creds):
        
        self.db_creds = db_creds


    def read_db_creds(self):
        '''
        This function reads the credentials from yaml file and returns the dictionary of credentials

        '''
        import yaml

        with open(self.db_creds, 'r') as stream:
            yaml_data = yaml.safe_load(stream)    
        return yaml_data

    def init_db_engine(self):
        '''
        This function will read the credentials from the return of read_db_creds and initialise and 
        return and sqlaclhemy databse engine..
        '''
        from sqlalchemy import create_engine

        yaml_data = self.read_db_creds()
        engine = create_engine(f"{yaml_data['DATABASE_TYPE']}://{yaml_data['RDS_USER']}:{yaml_data['RDS_PASSWORD']}@{yaml_data['RDS_HOST']}:{yaml_data['RDS_PORT']}/{yaml_data['RDS_DATABASE']}")
     #   engine.connect()
        return engine

    def list_db_tables(self):
        '''
        This function will list all the tables in a database so you know 
        which tables you can extract data from
        '''
        from sqlalchemy import inspect

        inspector = inspect(self.init_db_engine())
        tablet_list = inspector.get_table_names()
        return tablet_list
    
    def upload_to_db(self, table_name, pandas_dataframe):
        '''
        This method will upload the dataframe to the database.
        Will take pandas dataframe and table name as an argument
        '''
        pandas_dataframe.to_sql(table_name, self.init_db_engine(), if_exists = 'replace')

def load_all_db():
    '''
    This function loads dataframes into databases
    '''
    from data_cleaning import DataCleaning

    upload_to_db = DatabaseConnector('sales_db_creds.yaml')
    upload_to_db.upload_to_db('dim_users' , DataCleaning.clean_user_data())
    upload_to_db.upload_to_db('dim_card_details', DataCleaning.clean_card_details())
    upload_to_db.upload_to_db('dim_store_details', DataCleaning.called_clean_stored_data())
    upload_to_db.upload_to_db('dim_products', DataCleaning.clean_products_data())
    upload_to_db.upload_to_db('orders_table', DataCleaning.clean_order_details())
    upload_to_db.upload_to_db('dim_date_times', DataCleaning.clean_date_time_details())


load_all_db()
