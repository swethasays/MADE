import pytest
import pandas as pd
import os
from My_Pipeline import (transform_airports_germany, transform_airlines,
                         transform_airplanes, transform_global_airports,
                         transform_routes, run_etl_pipeline, datasets)

@pytest.fixture
def file1_airports_germany_data():
    return pd.DataFrame({
        'latitude_deg': ['52.5597', 'invalid_data'],
        'longitude_deg': ['13.2877', 'invalid_data'],
        'name': ['Berlin Tegel', 'Munich Airport'],
        'municipality': ['Berlin', 'Munich'],
        'iso_country': ['DE', 'DE'],
        'gps_code': [None, 'EDDM'],
        'iata_code': ['TXL', None]
    })

@pytest.fixture
def file2_airlines_data():
    return pd.DataFrame({
        'Name': ['Lufthansa', 'British Airways'],
        'Alias': ['LH', '\\N'],
        'Callsign': ['LUFTHANSA', '\\N'],
        'Country': ['Germany', 'United Kingdom'],
        'IATA': ['LH', 'BA'],
        'ICAO': ['DLH', 'BAW'],
        'Active': ['Y', 'N']
    })

@pytest.fixture
def file3_airplanes_data():
    return pd.DataFrame({
        'Name': ['Boeing 747', 'Airbus A320'],
        'IATA code': ['747', '32A'],
        'ICAO code': ['B744', 'A320']
    })

@pytest.fixture
def file4_airports_data():
    return pd.DataFrame({
        'Name': ['Los Angeles International', 'Heathrow'],
        'City': ['Los Angeles', 'London'],
        'Country': ['United States', 'United Kingdom'],
        'IATA': ['LAX', 'LHR'],
        'ICAO': ['KLAX', 'EGLL'],
        'Latitude': ['33.9425', '51.4775'],
        'Longitude': ['-118.4081', '-0.4614'],
        'Altitude': ['125', '83']
    })
  
@pytest.fixture
def file5_routes_data():
    return pd.DataFrame({
        'Airline': ['AA', 'BA'],
        'Source airport': ['JFK', 'LHR'],
        'Destination airport': ['LAX', 'JFK'],
        'Codeshare': ['Y', 'N'],
        'Stops': ['0', '1'],
        'Equipment': ['Boeing 747', 'Airbus A320']
    })

# Tests
def test_transform_airports_germany(file1_airports_germany_data):
    result = transform_airports_germany(file1_airports_germany_data)
    assert result['latitude_deg'].dtype == float, "Latitude column is not of type float"
    assert result['longitude_deg'].dtype == float, "Longitude column is not of type float"
    assert 'Berlin Tegel' in result['name'].values, "Berlin Tegel not found in name column"
    assert 'Munich Airport' in result['name'].values, "Munich Airport not found in name column"

def test_transform_airlines(file2_airlines_data):
    result = transform_airlines(file2_airlines_data)
    assert 'Lufthansa' in result['Name'].values, "Lufthansa not found in Name column"
    assert 'British Airways' in result['Name'].values, "British Airways not found in Name column"
    assert result['Active'].dtype == bool, "Active column is not of type bool"

def test_transform_airplanes(file3_airplanes_data):
    result = transform_airplanes(file3_airplanes_data)
    assert 'Boeing 747' in result['Name'].values, "Boeing 747 not found in Name column"
    assert 'Airbus A320' in result['Name'].values, "Airbus A320 not found in Name column"
    assert result['IATA code'].isnull().sum() == 0, "IATA code column contains null values"

def test_transform_global_airports(file4_airports_data):
    result = transform_global_airports(file4_airports_data)
    assert 'Los Angeles International' in result['Name'].values, "Los Angeles International not found in Name column"
    assert 'Heathrow' in result['Name'].values, "Heathrow not found in Name column"
    assert result['Latitude'].dtype == float, "Latitude column is not of type float"
    assert result['Longitude'].dtype == float, "Longitude column is not of type float"

def test_transform_routes(file5_routes_data):
    result = transform_routes(file5_routes_data)
    assert 'AA' in result['Airline'].values, "Airline 'AA' not found in Airline column"
    assert 'BA' in result['Airline'].values, "Airline 'BA' not found in Airline column"
    assert result['Stops'].dtype == int, "Stops column is not of type int"
    assert result['Equipment'].str.contains('Boeing').any(), "Boeing not found in Equipment column"

# System-level test
def test_etl_pipeline_execution():
    # Ensures output files do not pre-exist
    data_dir = "../data"
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    for dataset_name in datasets.keys():
        db_path = f"{data_dir}/{dataset_name}.sqlite"
        assert not os.path.exists(db_path), f"Output file already exists: {db_path}"

    # Executes the ETL pipeline
    run_etl_pipeline()

    # Check that each expected output file exists after pipeline execution
    for dataset_name in datasets.keys():
        db_path = f"{data_dir}/{dataset_name}.sqlite"
        assert os.path.isfile(db_path), f"Expected output file not found: {db_path}"
        os.remove(db_path)

if __name__ == "__main__":
    pytest.main()
