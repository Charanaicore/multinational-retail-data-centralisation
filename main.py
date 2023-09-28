from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector

if __name__ == "__main__":
    # instatiate classes for connecting to databases
    aws_connector = DatabaseConnector('aws_creds.yaml')
    local_connector = DatabaseConnector('local_creds.yaml')
    # instantiate class for extracting data from external sources
    extractor = DataExtractor()
    # instantiate class for cleaning data
    cleaner = DataCleaning()
    # extract users data and upload to local database
    users = extractor.read_rds_table(aws_connector, 'legacy_users')
    local_connector.upload_to_db(cleaner.clean_user_data(users), 'dim_users')
    # extract cards data and upload to local database
    cards = extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    local_connector.upload_to_db(cleaner.clean_card_data(cards), 'dim_card_details')
    # extract store data and upload to local database
    stores = extractor.retrieve_stores_data()
    local_connector.upload_to_db(cleaner.clean_store_data(stores), 'dim_store_details')
    # extract product data and upload to local database
    products = extractor.extract_from_s3('s3://data-handling-public/products.csv')
    local_connector.upload_to_db(cleaner.clean_products_data(products), 'dim_products')
    # extract orders data and upload to local database
    orders = extractor.read_rds_table(aws_connector, 'orders_table')
    local_connector.upload_to_db(cleaner.clean_orders_data(orders), 'orders_table')
    # extract order date and time event data and upload to local database
    date_events = extractor.extract_from_s3('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
    local_connector.upload_to_db(cleaner.clean_date_events(date_events), 'dim_date_times')

