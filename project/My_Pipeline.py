import pandas as pd
import sqlite3

datasets = {
    "Airports Germany": "https://query.data.world/s/xd45i2us5ic4eqikbfgsc5hexfvurk?dws=00000",
    "Global Airlines": "https://query.data.world/s/rucpufmwf7l5dgp5uxvhuniplfurxl?dws=00000",
    "Global Airplanes": "https://query.data.world/s/pc6c3caazxld7lwjvbcnci6eziol5q?dws=00000",
    "Global Airports": "https://query.data.world/s/rdfvb4syvuuwi3iwv7r3eyphvo7bsk?dws=00000",
    "Global Routes": "https://query.data.world/s/ehlhxizkunqbenzmuhrgymdfn3shh4?dws=00000"
}

for table_name, url in datasets.items():
    data = pd.read_csv(url)
    file_path = f"../data/{table_name.replace(' ', '_')}.sqlite"
    with sqlite3.connect(file_path) as conn:
        data.to_sql(table_name, conn, index=False, if_exists="replace")
    
    print(f"{table_name} dataset stored in SQLite database at {file_path}")

print("All datasets are created and stored in respective SQLite databases in the ../data directory.")
