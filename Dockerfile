# Use the official Apache Airflow image
FROM apache/airflow:2.5.0

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow

# Copy requirements file
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy DAGs and scripts into the container
COPY dags/ $AIRFLOW_HOME/dags/
COPY scripts/ $AIRFLOW_HOME/scripts/

# Set the entrypoint for the container
ENTRYPOINT ["/usr/local/bin/entrypoint"]
CMD ["webserver"]
