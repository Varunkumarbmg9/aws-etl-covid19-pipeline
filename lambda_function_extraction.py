import json
import boto3
import requests
import os
from datetime import datetime

# Initialize S3 client
s3=boto3.client('s3')

# Set variables
API_URL = "https://disease.sh/v3/covid-19/countries"
BUCKET_NAME = "covid-data-etl-project"

def lambda_handler(event, context):
    try:
        # Fetch COVID-19 data from the API
        response = requests.get(API_URL)
        data = response.json()

        filename = f"raw-data/covid_data_{datetime.now().strftime('%Y-%m-%d')}.json"

        s3.put_object(
            Bucket = BUCKET_NAME,
            Key = filename,
            Body = json.dumps(data)
        )

        return {
            'statusCode': 200,
            'body': json.dumps('Data fetched and stored successfully!')
        }
        # print("Data fetched and stored successfully!")
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps('Error fetching and storing data!')
        }
