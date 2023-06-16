import pandas as pd
import requests
import tabula
import boto3

class DataExtractor(object):
    
    @classmethod
    def read_rds_table(cls, table_name, db_conn):
        '''
        This function will take a table name as an argument and 
        return a pandas DataFrame.
        '''
        dataframe = pd.read_sql_table(table_name, db_conn.init_db_engine())
        return dataframe
    
    @classmethod
    def retrieve_pdf_data(cls):
        '''
        This function will take a pdf file and return pandas dataframe
        '''
        pdf_table_df = tabula.read_pdf('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf', pages = 'all', multiple_tables = False)
        return pdf_table_df[0]

    @classmethod
    def list_number_of_stores(cls):
        '''
        This method return the number of stores to extract. 
        It takes in the number of stores endpoint and header 
        dictionary as an argument
        '''
        api_key = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}

        # requests information and converts to json
        store_number = requests.get('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', headers= api_key).json()

        return store_number['number_stores']

    @classmethod
    def retrieve_stores_data(cls):
        '''
        This method return take and retrieve store endpoint as 
        an egument and extracts all the stores from the API.
        '''
        api_key = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        store_data_df = pd.DataFrame()
        number_of_stores = DataExtractor.list_number_of_stores()

        try:
            store_data_df = pd.read_csv('store_details.csv')
        except:
            print('File doesn\'t exist. Retrieving data from the web...')
        # for loop to iterate through all the pages and add them into the store_data_df dataframe
            for store_number in range(0, number_of_stores):
                store_data_json = requests.get(f"https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}", headers= api_key).json()
                store_data_df_single = pd.DataFrame.from_dict([store_data_json])
                store_data_df = pd.concat([store_data_df, store_data_df_single],ignore_index = True)
            store_data_df.to_csv('store_details.csv')

        return store_data_df
    
    @classmethod
    def extract_from_s3(cls):
        '''
        This method will use boto3 package to download and extract
        the information returning pandas Dataframe.
        '''
        s3_address = "s3://data-handling-public/products.csv"
        link_segments = s3_address.split('/')
        link_segments[0] = link_segments[0].replace(':','')
        link_segments.remove('')
        client = link_segments[0]
        folder = link_segments[-2]
        file = link_segments[-1]

        # Reads the file if it exists and runs boto3 client 
        # and tries to download the file if doesn't exist.
        try:    
            products_csv = pd.read_csv(file)
        except:
            print('File doesn\'t exist. Trying to download the file...')
            try:
                s3 = boto3.client(client)
                s3.download_file(folder, file, file)
                print('File downloaded. Run a code again.')
            except:
                print('Check link. Should look like s3://.../(filename)')

        return products_csv
      
    @classmethod
    def extract_json_from_s3(cls):
        '''
        This method will download json file from url and convert into dataframe.
        '''
        # Change this with your URL
        s3_address = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'

        json_file = requests.get(s3_address).json()
        datetime_df = pd.DataFrame.from_dict(json_file).dropna()

        return datetime_df
    

      

        





        
