import os
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
key = os.getenv("API_KEY")

def extract(city):
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={key}&units=imperial"
    response = requests.get(url)
    return response.json()

def transform(data):
    transformed_data = {
        "city": data["name"],
        "temperature": round(data["main"]["temp"]),
        "weather": data["weather"][0]["main"],
        "description": data["weather"][0]["description"]
    }
    return transformed_data

def load(data, filename):
    df = pd.DataFrame([data])
    df.to_csv(filename, index=False)

def run(city):
    data = extract(city)
    transformed = transform(data)
    load(transformed, "data.csv")

city = "Cedar Park"
run(city)
