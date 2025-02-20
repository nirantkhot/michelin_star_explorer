FROM apache/airflow:2.5.0

# Copy requirements file
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy DAGs and scripts
COPY dags/ /opt/airflow/dags/
COPY scripts/ /opt/airflow/scripts/
