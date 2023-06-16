class DataCleaning(object):
    
    @classmethod
    def clean_user_data(cls):
        '''
        This method uploads the dataframe from read_rds_table
        method from DataExtractor class in data_extraction.py file
        and clears all corrupted data by looking for 'NULL' 
        values in first_name cell and also by checking if dates are 
        correct. Returns "clean" dataframe with all corrupt columns deleted.
        '''

        from database_utils import DatabaseConnector
        from data_extraction import DataExtractor
        import pandas as pd

        users_table = DataExtractor().read_rds_table('legacy_users', DatabaseConnector('db_creds.yaml'))

        # Some useful functions to check overall information on the dataframe
        # users_table.dtypes
        # users_table.info()
        # users_table.head()
        # new_data.sum()

        if users_table['index'].is_unique == True:
            pass
        else:
            print('Some indexes have duplicates. Please check.')
            users_table['index_to_correct'] = users_table['index'].apply(lambda x: True 
                                                                         if x in users_table['index'].is_unique
                                                                         else False)
        users_table['first_name_issues'] = users_table['first_name'].apply(lambda x: pd.NaT 
                                                                           if 'NULL' in x
                                                                           else True)
        users_table = users_table[users_table['first_name_issues'].isin([True])]
        users_table['date_of_birth'] = pd.to_datetime(users_table['date_of_birth'], errors= 'coerce')
        users_table['join_date'] = pd.to_datetime(users_table['join_date'], errors= 'coerce')
        users_table = users_table.dropna().reset_index(drop = True)
        users_table_upd = users_table.drop(columns= ['first_name_issues', 'index'])

        return users_table_upd

    @classmethod
    def clean_card_details(cls):
        '''
        This method uploads the dataframe from retrieve_pdf_dataf method 
        from DataExtractor class in data_extraction.py file and clears all 
        corrupted data by looking for 'NULL' values in columns and also by 
        checking if dates are correct.
        '''
        from data_extraction import DataExtractor
        import pandas as pd
        from datetime import datetime
        import numpy as np

        def check_time(x):
            if x == 'date_payment_confirmed':
                x = np.NaN
            try:
                x = datetime.strptime(x, '%Y-%m-%d').date()
            except:
                try:
                    x = x.replace(' ','-')
                    x = datetime.strptime(x, '%Y-%B-%d').date()
                except:
                    try:
                        x = x.replace(' ','-')
                        x = datetime.strptime(x, '%B-%Y-%d').date()
                    except:
                        try:
                            x = x.replace('/','-')
                            x = datetime.strptime(x, '%Y-%m-%d').date()
                        except:
                            pass
                    return x
            return x
         
        clean_card_df = DataExtractor.retrieve_pdf_data()
        clean_card_df['date_payment_confirmed'] = pd.to_datetime(clean_card_df['date_payment_confirmed'], errors = 'ignore')
        clean_card_df['date_payment_confirmed'] = clean_card_df['date_payment_confirmed'].apply(check_time)
        clean_card_df['date_payment_confirmed'] = pd.to_datetime(clean_card_df['date_payment_confirmed'], errors = 'coerce')
        clean_card_df['card_number'] = clean_card_df['card_number'].astype(str)
        clean_card_df['card_number'] = clean_card_df['card_number'].apply(lambda x: x.replace('.', '') if '.' in x
                                                                            else x)
        clean_card_df = clean_card_df.dropna().reset_index(drop = True)
        
        return clean_card_df
    
    @classmethod
    def called_clean_stored_data(cls):
        '''
        This method uploads the dataframe from retrieve_stores_data method 
        from DataExtractor class in data_extraction.py file
        and clears all corrupted data by looking for 'NULL' values 
        in columns and also by checking if dates are correct.
        '''
        from data_extraction import DataExtractor
        import pandas as pd
        import numpy as np

        # removes all string characters from the row and leaves numbers only
        def clean_numbers_from_string(x):
            x = ''.join(i for i in x if i.isdigit())
            return x
            
        stores_table_api = DataExtractor.retrieve_stores_data()
        stores_table_api = stores_table_api.astype(str)
        stores_table_api['continent'] = stores_table_api['continent'].apply(lambda x: x.replace('ee', '') if 'ee' in x
                                                                            else x)
        stores_table_api['opening_date'] = pd.to_datetime(stores_table_api['opening_date'], errors = 'coerce')
        stores_table_api['staff_numbers'] = stores_table_api['staff_numbers'].apply(clean_numbers_from_string)
        stores_table_api = stores_table_api.dropna().reset_index(drop = True)
        stores_table_api['country_code'] = np.where(stores_table_api['address'] == 'nan', None, stores_table_api['country_code']) # looks for nan values in address and replaces with none in country_code
        stores_table_api['continent'] = np.where(stores_table_api['address'] == 'nan', None, stores_table_api['continent'])
        stores_table_api = stores_table_api.drop("lat", axis= 1) # same as .drop(columns= "lat")
        stores_table_api = stores_table_api.drop(columns = ['index', 'Unnamed: 0'])
        stores_table_api = stores_table_api.drop_duplicates(['address'])
        stores_table_api = stores_table_api.replace('nan', None)
        
        return stores_table_api

    @classmethod
    def convert_product_weights(cls):
        '''
        This method cleans weight column in the DataFrame products_df.
        Converts all grams to kg and all ml to g with 1:1 ratio.
        Removes all excess characters then represents the weights as float.
        '''
        from data_extraction import DataExtractor

        stores_df = DataExtractor.extract_from_s3()

        # function to be used to remove 'kg','g','ml','.' strings from dataframe
        # and convert g and ml into kg where g = ml and kg = g/1000 kg = ml/1000
        def check_weight(x):
            # converts grams into kgs
            if 'kg' in x:
                x = x.replace('kg', '')
            # converts grams into kgs
            elif 'g' in x:
                x = x.replace('g', '')
                x = x.replace('.', '')
                try:
                    x = float(x)
                    x = x / 1000 
                except:
                    # there are some columns with quanity x weight(grams) values
                    # this code below will split the values and multimply them
                    x = x.split('x')
                    x_0 = float(x[0])
                    x_1 = float(x[1])
                    x = x_0 * x_1
            # converts ml into grams
            elif 'ml' in x:
                x = x.replace('ml','')
                try:
                    x = float(x)
                    x = x / 1000 
                except:
                    pass
            # converts oz into grams
            elif 'oz' in x:
                x = x.replace('oz','')
                try:
                    x = float(x)
                    x = (x * 28.3495) / 1000
                except:
                    pass
            return x
    
        # drops all the columns with null data
        stores_df = stores_df.dropna().reset_index(drop = True)

        # checks the weight and converts it into kg
        stores_df['weight'] = stores_df['weight'].apply(check_weight)

        return stores_df
    
    @classmethod
    def clean_products_data(cls):
        '''
        This method cleans 'date_added' column and replaces all date related
        content. Returns clean dataframe with date_added column as type(datetime)
        '''
        import pandas as pd

        stores_df = DataCleaning.convert_product_weights()

        # function to convert date from a specific format
        def check_time(x):
            from datetime import datetime

            try:
                x = x.replace(' ','-')
                x = datetime.strptime(x, '%Y-%B-%d').date()
            except:
                try:
                    x = x.replace(' ','-')
                    x = datetime.strptime(x, '%B-%Y-%d').date()
                except:
                    pass
            return x
        
        # checks if date is correct and converts it if required
        stores_df['date_added'] = pd.to_datetime(stores_df['date_added'], errors = 'ignore') # errors = 'ignore' as some dates needs correction
        stores_df['date_added'] = stores_df['date_added'].apply(check_time) # uses check_time function to iterate through all the columns
        stores_df['date_added'] = pd.to_datetime(stores_df['date_added'], errors = 'coerce') # errors = 'coerce' to return all corrupted data as NaT
        stores_df['product_price'] = stores_df['product_price'].apply(lambda x: x.replace('£','') if '£' in x else x)
        stores_df = stores_df.drop(columns = 'Unnamed: 0')

        # drops all the columns with null data
        stores_df = stores_df.dropna().reset_index(drop = True)

        return stores_df

    @classmethod
    def clean_order_details(cls):
        '''
        This method uploads the dataframe from read_rds_table
        method from DataExtractor class in data_extraction.py file
        and clears all data. Removes first_name,last_name, level_0, and 1 columns
        This table is acting as the source of truth for sales.
        '''
        
        from database_utils import DatabaseConnector
        from data_extraction import DataExtractor

        orders_table = DataExtractor().read_rds_table('orders_table', DatabaseConnector('db_creds.yaml'))
        orders_table = orders_table.astype(str)
        orders_table = orders_table.drop(columns = ['first_name', '1', 'last_name', 'level_0'])
        orders_table['card_number'] = orders_table['card_number'].apply(lambda x: x.replace('.', '') if '.' in x
                                                                            else x)
        orders_table = orders_table.dropna().reset_index(drop = True)

        return orders_table
    
    @classmethod
    def clean_date_time_details(cls):
        '''
        This method uploads the dataframe from extract_json_from_s3 method 
        in DataExtractor class. Removes all corrupted data and returns clean
        dataframe
        '''
        from data_extraction import DataExtractor
        from datetime import datetime
        import numpy as np
        import pandas as pd

        date_time_df = DataExtractor.extract_json_from_s3()

        # function to replace all non time related data with NaN
        def check_time(x):
            try:
                x = datetime.strptime(x, '%H:%M:%S').time()
            except:
                x = np.NaN
            return x
        
        date_time_df['timestamp'] = date_time_df['timestamp'].apply(check_time)
        date_time_df['timestamp'] = pd.to_datetime(date_time_df['timestamp'], format = '%H:%M:%S', errors ='coerce').dt.time
        date_time_df = date_time_df.dropna().reset_index(drop = True)

        return date_time_df
