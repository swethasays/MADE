import urllib.request
import zipfile
import pandas as pd
import sqlite3


def download_and_extract(url, filename):
    try:
        file_path, _ = urllib.request.urlretrieve(url)
        with zipfile.ZipFile(file_path, 'r') as zip_file:
            zip_file.extract(filename)
        print("Download and extraction successful.")
    except Exception as e:
        print(f"Error during download and extraction: {e}")

def validate_stop_name(name):
    umlauts = ['ä', 'ö', 'ü', 'Ä', 'Ö', 'Ü', 'ß']
    return any(umlaut in name for umlaut in umlauts)

def validate_coordinates(lat, lon):
    lat_validate = -90 <= lat <= 90
    long_validate = -90 <= lon <= 90
    return lat_validate and long_validate


def load_and_process_data(file_name):
    columns_to_load = ['stop_id', 'stop_name', 'stop_lat', 'stop_lon', 'zone_id']

    df = pd.read_csv(file_name, usecols=columns_to_load)
    df_filtered = df[df['zone_id'] == 2001]

    
    #df_filtered = df_filtered['stop_name'].apply(validate_stop_name)
    
    # Validate and drop rows with invalid data
    df_filtered = df_filtered[df_filtered.apply(lambda x: validate_coordinates(x['stop_lat'], x['stop_lon']), axis=1)]

    return df_filtered

def write_to_sqlite(df, db_name='gtfs.sqlite', table_name='stops'):
    try:
        conn = sqlite3.connect(db_name)
        df.to_sql(table_name, conn, if_exists='replace', index=False, dtype={
            'stop_id': 'INTEGER',
            'stop_name': 'TEXT',
            'stop_lat': 'FLOAT',
            'stop_lon': 'FLOAT',
            'zone_id': 'INTEGER'
        })
        conn.close()
        print("Data written to SQLite database successfully.")
    except Exception as e:
        print(f"Error during writing to SQLite database: {e}")

def main():
    url = 'https://gtfs.rhoenenergie-bus.de/GTFS.zip'
    filename = 'stops.txt'

    download_and_extract(url, filename)
    processed_data = load_and_process_data(filename)
    write_to_sqlite(processed_data)


if __name__ == "__main__":
    main()
