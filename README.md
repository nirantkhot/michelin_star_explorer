# Michelin Star Explorer

A comprehensive data pipeline and analysis project exploring Michelin-starred restaurants worldwide.

## Dashboard
View our interactive dashboard: [Michelin Star Explorer Dashboard](https://liviaellen.com/michelin)

## Data Pipeline & Visualization

- Data is collected and processed through our ETL pipeline
- Results are stored in both Google BigQuery and MongoDB
- Interactive visualizations are created using Google Looker Studio
- Dashboard pulls data from both Google BigQuery and MongoDB for comprehensive analysis

![DDS Project drawio](https://github.com/user-attachments/assets/eaa8fecd-be59-4ee3-abc3-993f497e707c)

## Overview
This project is an Apache Airflow DAG that orchestrates tasks related to fetching, processing, and storing restaurant data in a MongoDB database.

## Summary of the Process

1. **Environment Setup**:
   - Loads environment variables using `dotenv`, specifically fetching the `GOOGLE_PLACES_API_KEY` required for Google Places API calls.

2. **Function Definitions**:
   - **`get_place_rating(name, address)`**: Fetches the Google Places rating and user ratings total for a given restaurant name and address.
   - **`json_to_mongo(json_data)`**: Inserts or updates restaurant data in a MongoDB collection.
   - **`csv_to_json(csv_data)`**: Converts CSV data into JSON format.
   - **`call_api()`**: Fetches CSV data from a specified GitHub URL containing restaurant information.
   - **`task_dag()`**: Calls the API, converts the CSV data to JSON, and uploads it to MongoDB.
   - **`google_dag()`**: Processes the CSV data to fetch Google ratings for each restaurant, updates the DataFrame, and uploads the results to MongoDB.
   - **`aggregate_michelin_data()`**: Aggregates restaurant data from MongoDB, calculating average ratings and counts of Michelin stars, and stores the results in a new collection.

3. **DAG Definition**:
   - The DAG is defined with default arguments, including the owner, start date, and retry settings.
   - Scheduled to run daily.

4. **Task Definitions**:
   - Several tasks are defined using `PythonOperator`:
     - `api_call_task`: Executes the `task_dag` function.
     - `google_dag_task`: Executes the `google_dag` function.
     - `aggregation_task`: Executes the `aggregate_michelin_data` function.
     - `aggregation_cuisine_task`: Executes the `aggregate_top_cuisines_by_rating` function (imported from another script).

5. **Task Dependencies**:
   - The tasks are set to run in a specific order: `api_call_task` runs first, followed by `google_dag_task`, and then both `aggregation_task` and `aggregation_cuisine_task` run in parallel.

## Setup Instructions

1. **Clone the Repository**:
   ```bash
   git clone git@github.com:yourusername/michelin_star_explorer.git
   cd michelin_star_explorer
   ```

2. **Create a Virtual Environment** (optional but recommended):
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   ```

3. **Install Required Packages**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Set Up Environment Variables**:
   - Create a `.env` file in the root directory of the project and add your Google Places API key:
     ```
     GOOGLE_PLACES_API_KEY=your_google_places_api_key_here
     ```

5. **Set Up MongoDB**:
   - Ensure you have MongoDB installed and running on your local machine or use a cloud MongoDB service.
   - Update the `MONGO_URI` in the code if necessary.

## How to Run the DAG

1. **Start Apache Airflow**:
   - Initialize the database:
     ```bash
     airflow db init
     ```
   - Start the web server:
     ```bash
     airflow webserver --port 8080
     ```
   - In a new terminal, start the scheduler:
     ```bash
     airflow scheduler
     ```

2. **Access the Airflow UI**:
   - Open your web browser and go to `http://localhost:8080`.
   - You should see the `daily_api_call` DAG listed.

3. **Trigger the DAG**:
   - Click on the DAG name to view its details.
   - You can manually trigger the DAG by clicking the "Trigger DAG" button.

4. **Monitor the Tasks**:
   - You can monitor the progress of each task in the Airflow UI.
