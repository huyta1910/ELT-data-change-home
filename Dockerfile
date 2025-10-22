# Dockerfile (Corrected for Python Version Alignment)

# CHANGE THIS LINE: Use a specific Python version tag for the base image.
# This guarantees the runtime environment matches our constraint file.
FROM apache/airflow:2.9.2-python3.8

# The rest of the file is correct and remains the same.
ARG AIRFLOW_VERSION="2.9.2"
ARG PYTHON_VERSION="3.8"

# Stage 1: Install System Dependencies
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    unixodbc-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
USER airflow

# Stage 2: Install Python Dependencies using the correct Constraint File
COPY requirements.txt /
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
    -r /requirements.txt

# Stage 3: Copy Your Project Code
COPY . /opt/airflow