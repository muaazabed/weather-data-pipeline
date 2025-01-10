import json
import os
from urllib.request import urlopen
from urllib.parse import urlencode
import boto3
import datetime

def lambda_handler(event, context):
    # Use environment variables to hold api key and city name
    key = os.getenv("API_KEY")
    city = os.getenv("CITY")
    
    # API request
    api_url = "https://api.openweathermap.org/data/2.5/weather"
    params = {"q": city, "appid": key, "units": "imperial"}
    query_string = urlencode(params)
    full_url = f"{api_url}?{query_string}"
    
    with urlopen(full_url) as response:
        weather_data = json.loads(response.read().decode('utf-8'))
    
    # Save to S3
    s3 = boto3.client('s3')
    timestamp = datetime.datetime.now().isoformat()
    s3.put_object(
        Bucket="weather-data-pipeline-muaaz",
        Key=f"raw-data/weather_{timestamp}.json",
        Body=json.dumps(weather_data)
    )
    
    return {"statusCode": 200, "body": "Data uploaded successfully"}
