import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Float, String, Text


data_url = "https://opendata.rhein-kreis-neuss.de/api/v2/catalog/datasets/rhein-kreis-neuss-flughafen-weltweit/exports/csv"


df = pd.read_csv(data_url, on_bad_lines='skip')

column_types = {
    'column_1': Integer(),
    'column_2': Text(),
    'column_3': Text(),
    'column_4': Text(),
    'column_5': Integer(),
    'column_6': Text(),
    'column_7': Float(),
    'column_8': Float(),
    'column_9': Integer(),
    'column_10': Float(),
    'column_11': Text(),
    'column_12': Text(),
    'geo_punkt': Text()
}


engine = create_engine('sqlite:///airports.sqlite')
metadata = MetaData()


airports = Table('airports', metadata,
                 *(Column(column_name, column_type) for column_name, column_type in column_types.items())
                 )


metadata.create_all(engine)


df.to_sql('airports', con=engine, if_exists='replace', index=False)
