from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import csv
import json
from io import StringIO
from datetime import datetime
from pymongo import MongoClient
import pandas as pd
import time
from typing import Optional
import os
from dotenv import load_dotenv
from scripts.michelin_cuisine_aggregation import aggregate_top_cuisines_by_rating
from google.cloud import storage

load_dotenv()

# Restaurant Github CSV data
REST_GITHUB_URL = os.getenv('REST_GITHUB_URL')
# Load MongoDB connection details from environment variables
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017')
DB_NAME = os.getenv('DB_NAME', 'msds-697-section-2')
COLLECTION_NAME = os.getenv('COLLECTION_NAME', 'testRestaurants')
GOOGLE_PLACES_API_KEY = os.getenv('GOOGLE_PLACES_API_KEY')
if not GOOGLE_PLACES_API_KEY:
    raise ValueError("Please set GOOGLE_PLACES_API_KEY in .env file")

# Define variables for GCS bucket and file name
MICHELIN_BUCKET_NAME = os.getenv('MICHELIN_BUCKET_NAME')  # Replace with your Michelin bucket name
MICHELIN_FILE_NAME = 'michelin_data.csv'  # Replace with your desired Michelin file name

def get_place_rating(name: str, address: str) -> Optional[float]:
    """Fetch Google Places rating for a restaurant."""
    base_url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"

    # Combine name and address for more accurate results
    query = f"{name} {address}"

    params = {
        'input': query,
        'inputtype': 'textquery',
        'fields': 'place_id,rating',
        'key': GOOGLE_PLACES_API_KEY
    }

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()

        if data['candidates']:
            place_id = data['candidates'][0].get('place_id')
            if place_id:
                # Get detailed place information
                details_url = "https://maps.googleapis.com/maps/api/place/details/json"
                details_params = {
                    'place_id': place_id,
                    'fields': 'rating,user_ratings_total',
                    'key': GOOGLE_PLACES_API_KEY
                }
                details_response = requests.get(details_url, params=details_params)
                details_response.raise_for_status()
                details_data = details_response.json()
                return details_data.get('result', {}).get('rating'), details_data.get('result', {}).get('user_ratings_total')
        return None
    except Exception as e:
        print(f"Error fetching rating for {name}: {str(e)}")
        return None




# Function to upllad data into Mongo collection
def json_to_mongo(client, db, collection, json_data):

    updated_count = 0
    inserted_count = 0
    # Insert JSON data into MongoDB collection
    for data in json_data:
        filter_criteria = {'Name': data['Name']}
        result = collection.update_one(filter_criteria, {'$set': data}, upsert=True)

        if result.matched_count > 0:
            updated_count+=1
        else:
            inserted_count+=1

    print(f"Inserted {inserted_count} records into MongoDB.")
    print(f"Updated {updated_count} records into MongoDB.")

# Function to convert CSV to JSON
def csv_to_json(csv_data):
    json_data = None
    if csv_data is not None:
        csv_file = StringIO(csv_data)
        csv_reader = csv.DictReader(csv_file)
        json_data = [row for row in csv_reader]
    return json_data

# Function to get data from Google Cloud Storage
def get_data_from_gcs(bucket_name: str, file_name: str) -> Optional[str]:
    """Fetch data from a specified GCS bucket and file using Google Cloud credentials."""
    # Load credentials from a specified file
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    client = storage.Client.from_service_account_json(credentials_path)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    try:
        data = blob.download_as_text()
        return data
    except Exception as e:
        print(f"Error fetching data from GCS: {str(e)}")
        return None

# Function to check if data already exists in MongoDB
def data_exists_in_mongo(mongo_uri: str, db_name: str, collection_name: str, filter_criteria: Optional[dict] = None) -> bool:
    """Check if any data exists in the specified MongoDB collection based on optional filter criteria."""
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]
    # Use the provided filter criteria or an empty dict if none is provided
    return collection.count_documents(filter_criteria or {}) > 0  # Returns True if documents exist, otherwise False

def fetch_github_data(source_url) -> Optional[str]:
    """Fetch data from GitHub."""
    try:
        response = requests.get(source_url)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Failed to download from GitHub: {e}")
        return None

def upload_to_gcs(data: str, bucket_name: str, file_name: str):
    """Upload data to Google Cloud Storage."""
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        blob.upload_from_string(data, content_type='text/csv')
        print("Data uploaded to GCS successfully.")
    except Exception as e:
        print(f"Failed to upload to GCS: {e}")

def download_from_gcs(bucket_name: str, file_name: str) -> Optional[str]:
    """Download data from Google Cloud Storage."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    try:
        data = blob.download_as_text()
        print("Data downloaded from GCS successfully.")
        return data
    except Exception as e:
        print(f"Error fetching data from GCS: {str(e)}")
        return None

def michelin_to_gcs(source_url,bucket_name, file_name):
    """Download data from GitHub and upload it to Google Cloud Storage."""
    csv_data = fetch_github_data(source_url)  # Call the new function to fetch data
    if csv_data:  # Proceed only if data was fetched successfully
        upload_to_gcs(csv_data, bucket_name, file_name)  # Upload to GCS

# Modify the call_api function to check for GCS data first
def call_api():
    # Download from GitHub and upload to GCS
    michelin_to_gcs(REST_GITHUB_URL,MICHELIN_BUCKET_NAME, MICHELIN_FILE_NAME )

    # Check if data exists in MongoDB
    if data_exists_in_mongo(MONGO_URI, DB_NAME, COLLECTION_NAME):
        print("Data already exists in MongoDB. Skipping API call.")
        return None  # Skip API call if data exists

    # Attempt to get data from GCS
    gcs_data = get_data_from_gcs(MICHELIN_BUCKET_NAME, MICHELIN_FILE_NAME)  # Use the new variable
    if gcs_data:
        return gcs_data  # Return GCS data if available

    # If GCS data is not available, make the API call
    try:
        response = requests.get(REST_GITHUB_URL)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"API request failed: {e}")
        return None

def task_dag():
    csv_data = call_api()
    json_data = csv_to_json(csv_data)
    json_to_mongo(json_data)
    print("DAG Task completed successfully")

def places_api_call():
    csv_data = call_api()
    if csv_data is not None:
        csv_file = StringIO(csv_data)
        df = pd.read_csv(csv_file)
        df['google_rating'] = None
        df['google_reviews'] = None
        for idx, row in df.iterrows():
            rating, reviews = get_place_rating(row['Name'], row['Address'])
            df.at[idx, 'google_rating'] = rating
            df.at[idx, 'google_reviews'] = reviews
            time.sleep(0.2)
            if idx % 10 == 0:
                print(f"Processed {idx} restaurants...")
        csv_output = StringIO()
        df.to_csv(csv_output, index=False)
        #upload to gcs

        json_data = csv_to_json(csv_output.getvalue())
        json_to_mongo(json_data)
    print(f"Google API DAG Task completed successfully")



def connect_to_mongo(MONGO_URI, DB_NAME, COLLECTION_NAME):
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    return client, db, collection

from pymongo import MongoClient

def aggregate_michelin_cuisine_context(client, db, collection , OUTPUT_COLLECTION="restaurants_per_cuisine", write_to_db=False):
    """
    Fetches Michelin cuisine context, including average Google rating and Michelin award distribution.
    If `write_to_db=True`, writes results into the OUTPUT_COLLECTION using $merge.
    """

    pipeline = [
        {
            "$group": {
                "_id": "$michelin_info.Cuisine",
                "google_rating": {"$avg": "$google_info.google_rating"},
                "michelin_rating": {
                    "three_stars": {"$sum": {"$cond": [{"$eq": ["$michelin_info.Award", "3 Stars"]}, 1, 0]}},
                    "two_stars": {"$sum": {"$cond": [{"$eq": ["$michelin_info.Award", "2 Stars"]}, 1, 0]}},
                    "one_star": {"$sum": {"$cond": [{"$eq": ["$michelin_info.Award", "1 Star"]}, 1, 0]}},
                    "bib_gourmand": {"$sum": {"$cond": [{"$eq": ["$michelin_info.Award", "Bib Gourmand"]}, 1, 0]}},
                    "selected": {"$sum": {"$cond": [{"$eq": ["$michelin_info.Award", "Selected"]}, 1, 0]}}
                },
                "total_restaurants": {"$sum": 1}
            }
        },
        {"$sort": {"google_rating": -1}}
    ]

    result = list(collection.aggregate(pipeline, allowDiskUse=True))

    # Print results
    for entry in result:
        print(entry)

    # Write to MongoDB if write_to_db=True
    if write_to_db:
        pipeline.append({
            "$merge": {
                "into": OUTPUT_COLLECTION,
                "whenMatched": "merge",
                "whenNotMatched": "insert"
            }
        })
        collection.aggregate(pipeline, allowDiskUse=True)
        print(f"Data written to '{OUTPUT_COLLECTION}' collection.")

    client.close()

    return result

from pymongo import MongoClient

def count_michelin_starred_restaurants(client, db, collection, output_collection="restaurants_per_city", write_to_db=False):
    """
    Counts restaurants per city & country, considering only 1, 2, or 3 Michelin stars.
    Excludes "Bib Gourmand" and "Selected".
    If `write_to_db=True`, writes results into the OUTPUT_COLLECTION using $merge.
    """


    pipeline = [
        {
            "$match": {
                "michelin_info.Location": { "$exists": True, "$ne": "" },
                "michelin_info.Award": { "$in": ["1 Star", "2 Stars", "3 Stars"] }  # Exclude "Bib Gourmand" & "Selected"
            }
        },
        {
            "$set": {
                "City": { "$arrayElemAt": [{ "$split": ["$michelin_info.Location", ", "] }, 0] },
                "Country": { "$arrayElemAt": [{ "$split": ["$michelin_info.Location", ", "] }, 1] }
            }
        },
        {
            "$group": {
                "_id": { "City": "$City", "Country": "$Country" },
                "total_restaurants": { "$sum": 1 }
            }
        },
        { "$sort": { "total_restaurants": -1 } }
    ]

    result = list(collection.aggregate(pipeline, allowDiskUse=True))

    # Print results
    for entry in result:
        print(entry)

    # Write to MongoDB if write_to_db=True
    if write_to_db:
        pipeline.append({
            "$merge": {
                "into": output_collection,
                "whenMatched": "merge",
                "whenNotMatched": "insert"
            }
        })
        collection.aggregate(pipeline, allowDiskUse=True)
        print(f"Data written to '{output_collection}' collection.")

    return result


def count_three_star_michelin_restaurants(client, db, collection, output_collection="three_star_restaurants_per_city", write_to_db=False):
    """
    Counts 3-star Michelin restaurants per city & country.
    If `write_to_db=True`, writes results into the OUTPUT_COLLECTION using $merge.
    """
    pipeline = [
        {
            "$match": { "michelin_info.Award": "3 Stars" }  # Only 3-Star restaurants
        },
        {
            "$set": {
                "City": { "$arrayElemAt": [{ "$split": ["$michelin_info.Location", ", "] }, 0] },
                "Country": { "$arrayElemAt": [{ "$split": ["$michelin_info.Location", ", "] }, 1] }
            }
        },
        {
            "$group": {
                "_id": { "City": "$City", "Country": "$Country" },
                "three_star_count": { "$sum": 1 }
            }
        },
        { "$sort": { "three_star_count": -1 } }
    ]

    result = list(collection.aggregate(pipeline, allowDiskUse=True))

    # Print results
    for entry in result:
        print(entry)

    # Write to MongoDB if write_to_db=True
    if write_to_db:
        pipeline.append({
            "$merge": {
                "into": output_collection,
                "whenMatched": "merge",
                "whenNotMatched": "insert"
            }
        })
        collection.aggregate(pipeline, allowDiskUse=True)
        print(f"Data written to '{output_collection}' collection.")

    return result


# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 18),  # Set your desired start date
    'retries': 1,
}

dag = DAG(
    'michelin_star_explorer',
    default_args=default_args,
    description='A simple DAG to call an API daily',
    schedule='@monthly',  # This schedule runs the DAG once a day
    catchup=False,  # Set to False to prevent backfilling
)

# Define the task
api_call_task = PythonOperator(
    task_id='task_dag',
    python_callable=task_dag,
    dag=dag,
)

places_api_call_task = PythonOperator(
    task_id='places_api_call',
    python_callable=places_api_call,
    dag=dag,
)

aggregation_task = PythonOperator(
    task_id='aggregate_michelin_data',
    python_callable=aggregate_michelin_data,
    dag=dag,
)

aggregation_cuisine_task = PythonOperator(
    task_id='aggregate_top_cuisines_by_rating',
    python_callable=aggregate_top_cuisines_by_rating,
    dag=dag,
)

api_call_task >> places_api_call_task >> [ aggregation_task, aggregation_cuisine_task]
