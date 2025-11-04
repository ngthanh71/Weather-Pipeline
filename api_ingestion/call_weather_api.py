import os
import sys
import requests
import pymongo
from datetime import datetime
from dotenv import load_dotenv


load_dotenv(dotenv_path="/opt/airflow/config.env")

API_KEY = os.getenv('WEATHER_API_KEY')
LOCATION = "Hanoi"

if not API_KEY:
    print("API_KEY is not set")

    import sys
    sys.exit(1)

MONGO_URI = os.getenv('MONGODB_URI')

if not MONGO_URI:
    MONGO_URI = 'mongodb://mongo:27017/'
    print("not found mongo")

try:
    client = pymongo.MongoClient(MONGO_URI)
    client.admin.command('ping')
    print("mongo connect success")
except pymongo.errors.ConfigurationError as e:
    print(f"error: {e}")
    import sys
    sys.exit(1) 
except Exception as e:
    print(f"mongo error: {e}")
    import sys
    sys.exit(1)


db = client["DataWeather"]
collection = db["forecast"]

def fetch_weather():
    url = f"https://api.weatherapi.com/v1/current.json?key={API_KEY}&q={LOCATION}"
    response = requests.get(url, timeout=15)

    if response.status_code == 403:
        raise Exception("API error 403")
    if response.status_code != 200:
        raise Exception(f"API error {response.status_code}: {response.text}")

    data = response.json()
    data["location_name"] = LOCATION
    data["fetched_at"] = datetime.utcnow()
    return data

def save_to_mongo(data):
    res = collection.insert_one(data)
    print(f"[{datetime.utcnow()}] Saved weather data for {LOCATION}, inserted_id={res.inserted_id}")
    return res.inserted_id

def main():
    print(f"weather data:{LOCATION}...")
    try:
        data = fetch_weather()
        save_to_mongo(data)
        print("Success!")
    except Exception as e:
        print(f"Error: {e}")
        import sys
        sys.exit(1)

if __name__ == "__main__":
    main()