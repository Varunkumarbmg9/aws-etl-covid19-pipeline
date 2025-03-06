import json
import csv
import boto3
from io import StringIO
from datetime import datetime

# Initialize S3 client
s3 = boto3.client('s3')

# S3 bucket details
BUCKET_NAME = "covid-data-etl-project"  # Replace with your actual bucket name
RAW_FOLDER = "raw-data/"
PROCESSED_FOLDER = "processed-data/"

def lambda_handler(event, context):
    try:
        print("Fetching latest JSON file from S3...")
        
        # Get the latest file in raw-data folder
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=RAW_FOLDER)
        
        if 'Contents' not in response:
            raise Exception("No files found in raw-data folder!")
        
        # Sort files by last modified time (latest first)
        latest_file = max(response['Contents'], key=lambda x: x['LastModified'])['Key']
        print(f"Latest file found: {latest_file}")

        # Read the JSON file from S3
        response = s3.get_object(Bucket=BUCKET_NAME, Key=latest_file)
        json_data = json.loads(response['Body'].read().decode('utf-8'))

        # Convert JSON to CSV
        csv_buffer = StringIO()
        csv_writer = csv.writer(csv_buffer)

        # Write CSV header
        csv_writer.writerow(["Country", "Cases", "Deaths", "Recovered"])

        for entry in json_data:
            csv_writer.writerow([entry["country"], entry["cases"], entry["deaths"], entry["recovered"]])

        # Generate CSV filename
        csv_filename = f"{PROCESSED_FOLDER}covid_data_{datetime.now().strftime('%Y-%m-%d')}.csv"

        # Upload CSV to S3
        s3.put_object(Bucket=BUCKET_NAME, Key=csv_filename, Body=csv_buffer.getvalue())

        print(f"CSV file saved successfully: {csv_filename}")

        return {
            "statusCode": 200,
            "body": f"CSV file successfully saved to S3 as {csv_filename}"
        }

    except Exception as e:
        print(f"Error during JSON to CSV conversion: {e}")
        return {
            "statusCode": 500,
            "body": f"Error: {str(e)}"
        }
