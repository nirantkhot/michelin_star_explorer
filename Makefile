# Makefile for setting up and running Apache Airflow DAG

# Variables
PYTHON_VERSION=3.10.6
VENV_NAME=michelin_af
REQUIREMENTS_FILE=requirements.txt
# AIRFLOW_HOME=$(shell pwd)
AIRFLOW_HOME=~/airflow
DAG_FILE=$(AIRFLOW_HOME)/dags/airflow_dag.py
VENV_DIR=$(AIRFLOW_HOME)/$(VENV_NAME)  # Directory for the virtual environment
MONGO_URI='mongodb://localhost:27017'

# Default target
.PHONY: all
all: setup venv install init start

# Setup Airflow environment
.PHONY: setup
setup:
	@echo "Setting up Airflow environment..."
	mkdir -p $(AIRFLOW_HOME)/dags
	mkdir -p $(AIRFLOW_HOME)/logs
	mkdir -p $(AIRFLOW_HOME)/plugins
	@echo "Airflow environment set up at $(AIRFLOW_HOME)."

# Create Python virtual environment using pyenv
.PHONY: venv
venv:
	@echo "Creating Python virtual environment with pyenv..."
	pyenv virtualenv $(PYTHON_VERSION) $(VENV_NAME)
	@echo "Virtual environment created at $(VENV_DIR)."

# Install dependencies
.PHONY: install
install: venv
	@echo "Installing Apache Airflow and required packages from $(REQUIREMENTS_FILE)..."
	@pyenv activate $VENV_NAME && \
	pip install -r $(REQUIREMENTS_FILE)

# Initialize Airflow database
.PHONY: init
init:
	@echo "Initializing Airflow database..."
	# @pyenv activate $(VENV_NAME)
	airflow db init

# Start Airflow web server and scheduler
.PHONY: start
start:
	echo $(AIRFLOW_HOME)
	@export AIRFLOW_HOME=$(AIRFLOW_HOME)
	@echo "Starting Airflow web server..."
	airflow webserver --port 8080 &
	@echo "Starting Airflow scheduler..."
	airflow scheduler &

# Clean up
.PHONY: clean
clean:
	@echo "Cleaning up..."
	killall airflow || true

# Run the DAG
.PHONY: run
run:
	@echo "Triggering the DAG..."
	airflow dags trigger daily_api_call
