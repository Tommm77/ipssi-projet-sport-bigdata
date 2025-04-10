#!/bin/bash

echo "Refreshing Airflow DAGs and updating configuration..."

# Stop the containers
echo "Stopping containers..."
docker-compose down kafka-notifications-producer airflow

# Start Airflow (without the disabled notifications producer)
echo "Starting Airflow..."
docker-compose up -d airflow

# Wait for Airflow to be ready
echo "Waiting for Airflow to be ready..."
sleep 10

# Trigger the DAG manually to start it immediately
echo "Triggering football_notifications DAG..."
docker exec airflow airflow dags unpause football_notifications

echo "Done! The football_notifications DAG is now the sole producer of notifications."
echo "You can check the Airflow UI at http://localhost:18080 to verify the DAG is running." 