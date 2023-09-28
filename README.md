# multinational-retail-data-centralisation

## Project brief
You work for a multinational company that sells various goods across the globe. Currently, their sales data is spread across 
many different data sources making it not easily accessible or analysable by current members of the team. In an effort to 
become more data-driven, your organisation would like to make its sales data accessible from one centralised location. Your 
first goal will be to produce a system that stores the current company data in a database so that it's accessed from one 
centralised location and acts as a single source of truth for sales data. You will then query the database to get up-to-date 
metrics for the business.

The different data sources that need to be extracted from and collected together are:
- two tables of an SQL database hosted on AWS RDS
- one table stored as a .pdf file hosted on AWS S3
- one table stored as a .csv file hosted on AWS S3
- one table stored as a .json file hosted on AWD S3
- a series of JSON objects available via an API

By completing this project, I have built a pipeline for extracting the data from the various sources, transforming (cleaning) 
the data, and loading the data into a new Postgresql database hosted locally. Once extracted and loaded, further transformation 
of the data and database was performed to complete the database schema. Finally, several SQL queries were written to enable 
users of the database to query the data and extract meaningful insights from it.

## Project Dependencies

In order to run this project, the following modules need to be installed:

- `pandas`
- `sqlalchemy`
- `requests`
- `tabula-py`
- `python-dotenv`
- `PyYAML`

If you are using Anaconda and virtual environments (recommended), the Conda environment can be cloned by running the following
command, ensuring that env.yml is present in the project:

`conda create env -f env.yml -n $ENVIRONMENT_NAME`

It's worth noting that the pipeline won't run as it is without the AWS credentials or API key, but the DatabaseConnector
and DataExtractor classes will work on other sources of data with amendments to API endpoints.

## Tools used

### SQLAlchemy
[SQLAlchemy](https://www.sqlalchemy.org/) was used to connect to both the AWS and local SQL databases. In `database_utils.py`:

```python
from sqlalchemy import create_engine, inspect
```

From the [SQLAlchemy documentation](https://docs.sqlalchemy.org/en/20/tutorial/engine.html):

> The start of any SQLAlchemy application is an object called the Engine. This object acts as a central source of connections 
> to a particular database, providing both a factory as well as a holding space called a connection pool for these database 
> connections. The engine is typically a global object created just once for a particular database server, and is configured 
> using a URL string which will describe how it should connect to the database host or backend.

For example:

```python
# Construct connection string
connection_string = f"postgresql+psycopg2://{db_username]}:{db_password}@" + f"{db_host]}:{db_port}/{db_database}"
# Create new sqlalchemy database engine
engine = create_engine(connection_string)
```

The `inspect()` method is used to get information about a connected database:

```python
inspector = inspect(engine)
table_name_list = inspector.get_table_names()
```

### PyYAML

The credentials for the databases are stored locally in YAML files. In order to access the credentials to pass into the 
create_engine() method above, [PyYAML](https://pyyaml.org/wiki/PyYAMLDocumentation) was used to read the YAML files and load 
the contents into a dictionary:

```python
import yaml

# Use context manager to open file
with open(self.filename, 'r') as file:
    # load contents into dictionary
    contents_dictionary = yaml.safe_load(file)
```

### pandas

[pandas](https://pandas.pydata.org/) is a fast, powerful, flexible and easy to use open source data analysis and manipulation tool,
built on top of the Python programming language. This project makes use of the pandas DataFrame, a two-dimensional data structure
(essentially a table) that using pandas built in methods makes it easy to search, manipulate and visualise large sets of data.

```python
# convention is to give pandas the alias pd
import pandas as pd
```

This project utilises many of pandas built in methods. A few common examples are listed below:

```python
# read SQL table from database connection established using SQLAlchemy
dataframe = pd.read_sql_table(table_name, engine)
# load DataFrame as new table into an existing database connection, don't load index, replace if already exists
dataframe.to_sql(new_table, engine, index=False, if_exists='replace')
# concatenate two DataFrames into one
new_dataframe = pd.concat([dataframe1, dataframe2], ignore_index=True)
# read contents of csv into DataFrame
dataframe = pd.read_csv('file.csv')
# read contents of json into DataFrame
dataframe = pd.read_json('file.json')
# drop a column from a DataFrame, inplace=True means action is performed on existing DataFrame
dataframe.drop('column_name', axis=1, inplace=True)
# drop any rows that contain null values
dataframe.dropna(inplace=True)
# convert a DataFrame column to datetime type
pd.to_datetime(dataframe['column_name'])
# apply a function (can be a lambda function) to a DataFrame or DataFrame column
dataframe['column_name'] = dataframe['column_name'].apply(function)
# reset the index
dataframe.reset_index(inplace=True)
# cast a column to a different data type
dataframe['column_name'].astype(str)
```

### Tabula

[Tabula](https://tabula-py.readthedocs.io/en/latest/#) is a simple tool for reading tables from pdf files and converting them
to a pandas DataFrame or CSV/TSV/JSON file.

```python
import tabula

dataframe = tabula.read_pdf(link, pages='all')
# depending on the table format, you may need to reset the index of the pandas DataFrame
dataframe.reset_index(inplace=True)
```

### Requests

In order to connect to API endpoints, [Requests](https://pypi.org/project/requests/) was used to make HTTPS GET requests.

```python
import requests

# make HTTPS GET request using URL of API endpoint and any necessary headers, i.e. x-api-key
response = requests.get({API_URL}, headers={HEADER_DICTIONARY})
# convert JSON response to pandas DataFrame
new_dataframe = pd.DataFrame(response.json(), index=[0])
```

### python-dotenv

When hosting code on Github or any other public repository, it's a good idea to keep any API keys or database credentials
separate from the hosted code. This can be done by using a .env file that is added to the .gitignore.
[python-dotenv](https://pypi.org/project/python-dotenv/) is then used to access any environment variables stored in the .env
file.

In the .env file:

```python
API_HEADER=mysecretapikey
```

In the Python script:

```python
import os
from dotenv import load_dotenv

api_header = {'x-api-key': os.getenv("API_HEADER")}
```

## Setting up a local database

A local Postgres database was set up to receive the cleaned data from the different sources. Postgres was installed globally
using Homebrew:

`brew install postgresql@14`

The local database was created using the following command in the terminal:

`initdb -D db -U postgres -W`

where `db` is the directory containing the database files and `postgres` is the database username. The `-W` flag indicates that
the database will be password protected and the user is prompted to enter the password upon running this command.

The database can be started using:

`postgres -D db`

## Connecting to the local database using pgAdmin

[pgAdmin](https://www.pgadmin.org/) is used to connect to the local database. With pgAdmin installed and running, follow these
steps to connect:

1. On the main application page, click on 'Add New Server'

![Add server image](/images/Add_server.png)

2. On the 'General' tab of the dialogue that appears, enter a name for the new server connection

![General tab image](/images/Options1.png)

3. On the 'Connection' tab, enter 'localhost' for the 'Host name/address', and enter the username and password specified when
creating the database.

![](/images/Options2.png)

4. Click 'Save' to save the server and connect to the database.

## Project structure

The project consists of three main classes, each with separate functions:

- `DatabaseConnector` - in `database_utils.py` - contains all methods necessary for connecting and uploading to SQL databases
- `DataExtractor` - in `data_extraction.py` - contains all methods necessary for retrieving data from various sources
- `DataCleaning` - `data_cleaning.py` - contains all methods necessary for cleaning individual pandas DataFrames

## Running the pipeline

Running `main.py` will create the necessary instances of the three classes listed above, and sequentially extracts, cleans and
loads data to the local database.

## SQL Queries

The project also contains two files with a series of SQL queries, `database_schema.sql` and `business_queries.sql`. The first
files contains queries that alter tables, such as changing data types and adding primary and foreign keys. The second file
contains queries for extracting insights from the data, such as finding out how certain types of store are performing in a
particular country or which months produce the highest volume of sales.

## Next steps

For the future direction of this project, I'd like to learn more about SQLAlchemy in order to be able to integrate running the
SQL queries via the main pipeline script. It would also be a good idea to perform more error checking/handling on the database/
API connection processes.




