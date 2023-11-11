import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Float, String, Text


data_url = "https://opendata.rhein-kreis-neuss.de/api/v2/catalog/datasets/rhein-kreis-neuss-flughafen-weltweit/exports/csv"
df = pd.read_csv(data_url, delimiter=';', on_bad_lines='skip')
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


engine = create_engine('sqlite:///airports.sqlite')
metadata = MetaData()
airports = Table('airports', metadata,
                 *(Column(column_name, column_type) for column_name, column_type in column_types.items())
                 )
metadata.create_all(engine)
df.to_sql('airports', con=engine, if_exists='replace', index=False)
