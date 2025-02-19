from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import csv
import json
from io import StringIO
from datetime import datetime
from pymongo import MongoClient

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

# Set task dependencies (in this case, just one task, so no dependencies)
api_call_task