import pandas as pd
import sqlite3

datasets = {
    "Airports Germany": "https://query.data.world/s/sjwvpaxrfji5s5mnwexv4zisexgui7?dws=00000",
    "Global Airlines": "https://query.data.world/s/rucpufmwf7l5dgp5uxvhuniplfurxl?dws=00000",
    "Global Airplanes": "https://query.data.world/s/pc6c3caazxld7lwjvbcnci6eziol5q?dws=00000",
    "Global Airports": "https://query.data.world/s/rdfvb4syvuuwi3iwv7r3eyphvo7bsk?dws=00000",
    "Global Routes": "https://query.data.world/s/ehlhxizkunqbenzmuhrgymdfn3shh4?dws=00000"
}

# Function to retrieve data from a given URL
def extract_data(url):
    try:
        return pd.read_csv(url)
    except pd.errors.ParserError as e:
        raise ValueError(f"Error parsing CSV data: {e}")



# Function to apply specific transformations to the 'Airports in Germany' dataset
def transform_airports_germany(data):
    transformed = data.iloc[1:].copy()
    transformed['latitude_deg'] = pd.to_numeric(transformed['latitude_deg'], errors='coerce')
    transformed['longitude_deg'] = pd.to_numeric(transformed['longitude_deg'], errors='coerce')
    transformed['name'] = transformed['name'].str.strip()
    transformed['municipality'] = transformed['municipality'].str.strip()
    transformed['iso_country'] = transformed['iso_country'].str.strip()
    transformed['gps_code'] = transformed['gps_code'].fillna('Unknown')
    transformed['iata_code'] = transformed['iata_code'].fillna('Unknown')
    return transformed

def transform_airlines(data):
    transformed = data.copy()
    transformed['Name'] = transformed['Name'].str.strip()
    transformed['Alias'] = transformed['Alias'].replace({'\\N': None}).str.strip()
    transformed['Callsign'] = transformed['Callsign'].replace({'\\N': None}).str.strip()
    transformed['Country'] = transformed['Country'].replace({'\\N': None}).str.strip()
    transformed['IATA'] = transformed['IATA'].replace({'\\N': None, '-': None})
    transformed['ICAO'] = transformed['ICAO'].replace({'\\N': None})
    transformed['Active'] = transformed['Active'].map({'Y': True, 'N': False})
    return transformed

def transform_airplanes(data):
    transformed = data.copy()
    transformed['Name'] = transformed['Name'].str.strip()
    transformed['IATA code'] = transformed['IATA code'].replace({'\\N': None})
    transformed['ICAO code'] = transformed['ICAO code'].replace({'\\N': None})
    return transformed

def transform_global_airports(data):
    transformed = data.copy()
    transformed['Name'] = transformed['Name'].str.strip()
    transformed['City'] = transformed['City'].str.strip()
    transformed['Country'] = transformed['Country'].str.strip()
    transformed['Type'] = transformed['Type'].str.strip()
    transformed['Source'] = transformed['Source'].str.strip()
    transformed['IATA'] = transformed['IATA'].replace({'\\N': None})
    transformed['ICAO'] = transformed['ICAO'].replace({'\\N': None})
    transformed['Latitude'] = pd.to_numeric(transformed['Latitude'], errors='coerce')
    transformed['Longitude'] = pd.to_numeric(transformed['Longitude'], errors='coerce')
    transformed['Altitude'] = pd.to_numeric(transformed['Altitude'], errors='coerce')
    return transformed

def transform_routes(data):
    transformed = data.copy()
    transformed['Airline'] = transformed['Airline'].str.strip()
    transformed['Source airport'] = transformed['Source airport'].str.strip()
    transformed['Destination airport'] = transformed['Destination airport'].str.strip()
    transformed['Equipment'] = transformed['Equipment'].str.strip()
    transformed['Codeshare'] = transformed['Codeshare'].fillna('No')
    transformed['Stops'] = pd.to_numeric(transformed['Stops'], errors='coerce')
    return transformed

# Function to decide which transformation to apply based on the dataset
def transform_data(data, dataset_name):
    if dataset_name == "Airports_Germany":
        return transform_airports_germany(data)
    elif dataset_name == "Global_Airlines":
        return transform_airlines(data)
    elif dataset_name == "Global_Airplanes":
        return transform_airplanes(data)
    elif dataset_name == "Global_Airports":
        return transform_global_airports(data)
    elif dataset_name == "Global_Routes":
        return transform_routes(data)
    return data

# Function to load data into a SQLite database
def load_data(data, dataset_name, db_file_path):
    with sqlite3.connect(db_file_path) as connection:
        data.to_sql(dataset_name, connection, if_exists='replace', index=False)

# Main loop to process each dataset
def run_etl_pipeline():
    for dataset_name, url in datasets.items():
        try:
            raw_data = extract_data(url)
            processed_data = transform_data(raw_data, dataset_name)
            db_path = f"../data/{dataset_name}.sqlite"
            load_data(processed_data, dataset_name, db_path)
            print(f"Data for {dataset_name} has been processed and stored in {db_path}")
        except Exception as e:
            print(f"Error processing {dataset_name}: {e}")

if __name__ == "__main__":
    run_etl_pipeline()
    print("ETL process completed for all datasets.")
