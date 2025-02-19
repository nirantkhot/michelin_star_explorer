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

load_dotenv()

GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
if not GOOGLE_API_KEY:
    raise ValueError("Please set GOOGLE_API_KEY in .env file")

def get_place_rating(name: str, address: str) -> Optional[float]:
    """Fetch Google Places rating for a restaurant."""
    base_url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
    
    # Combine name and address for more accurate results
    query = f"{name} {address}"
    
    params = {
        'input': query,
        'inputtype': 'textquery',
        'fields': 'place_id,rating',
        'key': GOOGLE_API_KEY
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
                    'key': GOOGLE_API_KEY
                }
                details_response = requests.get(details_url, params=details_params)
                details_response.raise_for_status()
                details_data = details_response.json()
                return details_data.get('result', {}).get('rating'), details_data.get('result', {}).get('user_ratings_total')
        return None
    except Exception as e:
        print(f"Error fetching rating for {name}: {str(e)}")
        return None


# Restaurant Github CSV data
REST_GITHUB_URL = "https://raw.githubusercontent.com/ngshiheng/michelin-my-maps/refs/heads/main/data/michelin_my_maps.csv"

# MongoDB connection details
MONGO_URI = 'mongodb://localhost:27017'
DB_NAME = 'msds-697-section-2'
COLLECTION_NAME = 'testRestaurants'

# Function to upllad data into Mongo collection
def json_to_mongo(json_data):
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    
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

# Define the function that calls the API
def call_api():
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

def google_dag():
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
        json_data = csv_to_json(csv_output.getvalue())
        json_to_mongo(json_data)
    print(f"Google API DAG Task completed successfully")
 


# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 18),  # Set your desired start date
    'retries': 1,
}

dag = DAG(
    'daily_api_call',
    default_args=default_args,
    description='A simple DAG to call an API daily',
    schedule='@daily',  # This schedule runs the DAG once a day
    catchup=False,  # Set to False to prevent backfilling
)

# Define the task
api_call_task = PythonOperator(
    task_id='task_dag',
    python_callable=call_api,
    dag=dag,
)

google_dag_task = PythonOperator(
    task_id='google_dag',
    python_callable=google_dag,
    dag=dag,
)

# Set task dependencies (in this case, just one task, so no dependencies)
api_call_task >> google_dag_task