import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Float, String, Text

#URL to fetch the data
data_url = "https://opendata.rhein-kreis-neuss.de/api/v2/catalog/datasets/rhein-kreis-neuss-flughafen-weltweit/exports/csv"

#Read the CSV data into a pandas dataframe, handling any bad lines by skipping them
df = pd.read_csv(data_url, delimiter=';', on_bad_lines='skip')

#Define the data types for each column in the dataframe
column_types = {
    'column_1': Integer(),
    'column_2': String(),
    'column_3': String(),
    'column_4': String(),
    'column_5': String(),  
    'column_6': String(),  
    'column_7': Float(),
    'column_8': Float(),
    'column_9': Integer(),
    'column_10': Float(),
    'column_11': String(),  
    'column_12': String(),  
    'geo_punkt': String()   
}

#create a connection to the SQLite database
engine = create_engine('sqlite:///airports.sqlite')

#Intiliaze database metadata
metadata = MetaData()

#Define the structure of the 'airports' table
airports = Table('airports', metadata,
                 *(Column(column_name, column_type) for column_name, column_type in column_types.items())
                 )
#Create the table in the database
metadata.create_all(engine)

#Write the dataframe to the 'airports' table in the database, overwriting the table if it already exists
df.to_sql('airports', con=engine, if_exists='replace', index=False)
