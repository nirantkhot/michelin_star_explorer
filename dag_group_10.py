from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pymongo import MongoClient
from google.cloud import storage
import requests
import json
import os
import csv
from io import StringIO
import pandas as pd
import time
from dotenv import load_dotenv
from typing import Optional
from airflow.utils.email import send_email  # Import email utility if using email notifications

# Load environment variables
load_dotenv()

# Environment Variables
REST_GITHUB_URL = os.getenv('REST_GITHUB_URL')
MONGO_URI = os.getenv('MONGO_URI')
DB_NAME = os.getenv('DB_NAME')
COLLECTION_NAME = os.getenv('COLLECTION_NAME')
CREDENTIALS_PATH = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
GOOGLE_PLACES_API_KEY = os.getenv('GOOGLE_PLACES_API_KEY')
MICHELIN_BUCKET_NAME = os.getenv('MICHELIN_BUCKET_NAME')
GOOGLE_DATA_NAME = os.getenv('GOOGLE_DATA_NAME')
MICHELIN_DATA_NAME = os.getenv('MICHELIN_DATA_NAME')
MICHELIN_GOOGLE_DATA_NAME = os.getenv('MICHELIN_GOOGLE_DATA_NAME')
JSON_MERGED_DATA_NAME = os.getenv('JSON_MERGED_DATA_NAME')
if not GOOGLE_PLACES_API_KEY:
    raise ValueError("Please set GOOGLE_PLACES_API_KEY in .env file")

# Initialize Google Cloud Storage Client
storage_client = storage.Client.from_service_account_json(CREDENTIALS_PATH)

### **MongoDB & GCS Utility Functions**
def check_mongo_collection():
    """Check if the MongoDB collection exists."""
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    return COLLECTION_NAME in db.list_collection_names()

def load_json_to_mongo():
    """Step 2: Load `michelin_google_mongo.json` from GCS into MongoDB."""
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    bucket = storage_client.bucket(MICHELIN_BUCKET_NAME)
    blob = bucket.blob("michelin_google_mongo.json")

    if blob.exists():
        json_data = json.loads(blob.download_as_string())
        collection.insert_many(json_data)
        print("Loaded `michelin_google_mongo.json` into MongoDB.")
        return True
    return False

### **Google Places API for Ratings**
def get_place_rating(name: str, address: str) -> Optional[float]:
    """Fetch Google Places rating for a restaurant."""
    base_url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
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

def download_from_gcs(bucket_name: str, file_name: str, credentials_path: str) -> str:
    """Download a file from Google Cloud Storage."""
    client = storage.Client.from_service_account_json(credentials_path)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    if blob.exists():
        return blob.download_as_text()
    else:
        print(f"File {file_name} does not exist in bucket {bucket_name}.")
        return None

def check_file_in_gcs(file_name: str) -> Optional[str]:
    """Check if a file exists in Google Cloud Storage."""
    client = storage.Client.from_service_account_json(CREDENTIALS_PATH)
    bucket = client.bucket(MICHELIN_BUCKET_NAME)
    blob = bucket.blob(file_name)

    return blob.exists()

def clean_data():
    """
    Merge the Michelin and Google Places datasets on 'Name' and
    save the merged data in a structured JSON format to a file.
    """
    # Define necessary variables
    bucket_name = MICHELIN_BUCKET_NAME  # Assuming this is the intended bucket name
    credentials_path = CREDENTIALS_PATH  # Use the existing credentials path
    # Download the data from GCS
    michelin_google_from_gcs = download_from_gcs(bucket_name, MICHELIN_GOOGLE_DATA_NAME, credentials_path)
    michelin_data_from_gcs = download_from_gcs(bucket_name, MICHELIN_DATA_NAME, credentials_path)
    google_data_from_gcs = download_from_gcs(bucket_name, GOOGLE_DATA_NAME, credentials_path)

    # Merge the datasets on 'Name'
    try:
        michelin_google = pd.read_csv(michelin_google_from_gcs)
        merged_data = michelin_google
    except:
        michelin_data = pd.read_csv(michelin_data_from_gcs)
        google_data = pd.read_csv(google_data_from_gcs)
        merged_data = pd.merge(michelin_data, google_data, on="Name", how="inner")

    # Replace NaN values with None for MongoDB JSON
    merged_data = merged_data.replace({pd.NA: None, float("nan"): None})

    # Convert DataFrame to structured JSON
    structured_data = []

    for _, row in merged_data.iterrows():
        structured_entry = {
            "Name": row["Name"],
            "michelin_info": {
                "Address": row["Address"],
                "Location": row["Location"],
                "Price": row["Price"],
                "Cuisine": row["Cuisine"],
                "Longitude": row["Longitude"],
                "Latitude": row["Latitude"],
                "PhoneNumber": row["PhoneNumber"],
                "Url": row["Url"],
                "WebsiteUrl": row["WebsiteUrl"],
                "Award": row["Award"],
                "GreenStar": int(row["GreenStar"]),
                "FacilitiesAndServices": row["FacilitiesAndServices"],
                "Description": row["Description"],
            },
            "google_info": {
                "google_rating": row["google_rating"],
                "google_reviews": row["google_reviews"],
            },
        }
        structured_data.append(structured_entry)

    # Convert to JSON format
    json_output = json.dumps(structured_data, indent=4, ensure_ascii=False)

    # Save to file
    json_file_path = JSON_MERGED_DATA_NAME
    with open(json_file_path, "w") as json_file:
        json_file.write(json_output)

    # Upload the JSON file to GCS
    upload_to_gcs(json_output,JSON_MERGED_DATA_NAME)

### **Places API Call for Google Ratings**
def places_api_call():
    """Fetch Google ratings and update MongoDB."""
    csv_data = check_file_in_gcs(GOOGLE_DATA_NAME)
    if csv_data is None:
        print("No CSV found in GCS.")
        csv_file = StringIO(csv_data)
        df = pd.read_csv(csv_file)
        df['google_rating'] = None
        df['google_reviews'] = None

        for idx, row in df.iterrows():
            rating, reviews = get_place_rating(row['Name'], row['Address'])
            df.at[idx, 'google_rating'] = rating
            df.at[idx, 'google_reviews'] = reviews
            time.sleep(0.2)  # API rate limit handling
            if idx % 10 == 0:
                print(f"Processed {idx} restaurants...")

        csv_output = StringIO()
        df.to_csv(csv_output, index=False)
        print("Google API DAG Task completed successfully.")
        return csv_output.getvalue()
    else:
        print("CSV found in GCS.")
        return csv_data

### **Aggregation Functions**

def count_restaurant_per_cuisine(client, db, collection , OUTPUT_COLLECTION="restaurants_per_cuisine", write_to_db=False):
    """
    Fetches Michelin cuisine context, including average Google rating and Michelin award distribution.
    If `write_to_db=True`, writes results into the OUTPUT_COLLECTION using $merge.
    """

    pipeline = [
        {
            "$group": {
                "_id": "$michelin_info.Cuisine",
                "google_rating": {"$avg": "$google_info.google_rating"},
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



    return result


def count_restaurant_per_cuisines(client, db, collection , OUTPUT_COLLECTION="restaurants_per_cuisine", write_to_db=False):
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



    return result


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

def run_aggregation(write_to_db=False):
    """Final step: Run aggregation queries."""
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    count_restaurant_per_cuisine(client, db, collection, write_to_db=write_to_db)
    count_michelin_starred_restaurants(client, db, collection, write_to_db=write_to_db)
    count_three_star_michelin_restaurants(client, db, collection, write_to_db=write_to_db)
    client.close()
    print("Aggregation completed successfully")

### **Notification Function**
def notify(message: str):
    """Send a notification with the given message."""
    print(message)  # Replace with your notification logic
    # Example: send_email(to='your_email@example.com', subject='DAG Notification', html_content=message)

### **Updated Workflow Functions**
def fetch_mongo_data():
    """Check MongoDB collection and run aggregation if it exists."""
    if not check_mongo_collection():
        notify("Collection does not exist. Checking GCS for michelin_google_mongo.json...")
        if check_file_in_gcs("michelin_google_mongo.json"):
            notify("michelin_google_mongo.json exists. Loading into MongoDB...")
            load_json_to_mongo()
        else:
            notify("michelin_google_mongo.json does not exist. Checking for CSV files...")
            check_and_fetch_csv_files()  # Proceed to check CSV files
    else:
        notify("Collection exists. Running aggregation...")


def check_and_fetch_csv_files():
    """Check for CSV files and fetch if missing."""
    google_data_exists = check_file_in_gcs(GOOGLE_DATA_NAME)
    michelin_data_exists = check_file_in_gcs(MICHELIN_DATA_NAME)

    if not michelin_data_exists:
        notify("michelin_data.csv is missing. Fetching from GitHub...")
        fetch_and_upload_michelin_data()

    if not google_data_exists:
        notify("google_data.csv is missing. Fetching from Google API...")
        fetch_and_upload_google_data()

    # Check if both CSVs exist after fetching
    google_data_exists = check_file_in_gcs(GOOGLE_DATA_NAME)
    michelin_data_exists = check_file_in_gcs(MICHELIN_DATA_NAME)

    if google_data_exists and michelin_data_exists:
        notify("Both CSVs exist. Preprocessing data...")
        clean_data()  # Call your existing clean function
        load_json_to_mongo()  # Load JSON to MongoDB
        run_aggregation()  # Run aggregation
        notify("Aggregation completed successfully.")

def upload_to_gcs(data: str, file_name: str):
    """Upload a string to Google Cloud Storage."""
    client = storage.Client.from_service_account_json(CREDENTIALS_PATH)
    bucket = client.bucket(MICHELIN_BUCKET_NAME)
    blob = bucket.blob(file_name)

    blob.upload_from_string(data)
    print(f"Uploaded {file_name} to GCS.")

def fetch_and_upload_michelin_data():
    """Fetch Michelin data from GitHub and upload it to GCS."""
    # Example implementation: Replace with actual fetching logic
    michelin_data = requests.get(REST_GITHUB_URL).text
    upload_to_gcs(michelin_data, MICHELIN_DATA_NAME)

def fetch_and_upload_google_data():
    """Fetch Google data using places_api_call and upload it to GCS."""
    # Call the places_api_call function to get the Google data
    csv_output = places_api_call()  # Assuming places_api_call returns the CSV data as a string
    upload_to_gcs(csv_output, MICHELIN_GOOGLE_DATA_NAME)

def notify_success():
    """Notify when the DAG completes successfully."""
    print("DAG completed successfully.")
    notify("DAG completed successfully.")

### **DAG Setup**
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 18),
    'retries': 1,
}

dag = DAG(
    'michelin_star_explorer',
    default_args=default_args,
    description='A DAG for Michelin Star data processing',
    schedule='@monthly',
    catchup=False,
)

# **Airflow Task Definitions**
task_fetch_mongo_data = PythonOperator(
    task_id='fetch_mongo_data',
    python_callable=fetch_mongo_data,
    dag=dag,
)

task_run_aggregation = PythonOperator(
    task_id='task_run_aggregation',
    python_callable=run_aggregation,
    dag=dag,
)

task_notify_success = PythonOperator(
    task_id='notify_success',
    python_callable=notify_success,
    dag=dag,
)

# **DAG Execution Flow**
task_fetch_mongo_data >> task_run_aggregation >> task_notify_success
