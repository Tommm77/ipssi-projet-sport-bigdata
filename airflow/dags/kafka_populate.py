from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from datetime import datetime, timedelta
from confluent_kafka import Producer
import random
import json

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Kafka config
KAFKA_CONFIG = {
    'bootstrap.servers': 'kafka:9092',  # change if needed
}
KAFKA_TOPIC = 'topic1'

def send_random_data_to_kafka():
    producer = Producer(KAFKA_CONFIG)

    def delivery_report(err, msg):
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    produits = [
    {"produit_id": 1, "nom": "Smartphone", "categorie": "Electronique", "prix": 599.99},
    {"produit_id": 2, "nom": "Laptop", "categorie": "Electronique", "prix": 1299.99},
    {"produit_id": 3, "nom": "Casque audio", "categorie": "Accessoires", "prix": 149.99},
    {"produit_id": 4, "nom": "Livre", "categorie": "Culture", "prix": 19.99},
    {"produit_id": 5, "nom": "Chaussures de sport", "categorie": "Mode", "prix": 89.99}
]
    for i in produits:
        message = json.dumps(i)
        producer.produce(KAFKA_TOPIC, value=message, callback=delivery_report)
        producer.flush()

with DAG(
    'kafka_pipeline',
    default_args=default_args,
    description='Hive + Kafka pipeline example',
    schedule_interval=None,  # Run manually or change to cron
    catchup=False,
) as dag:

    # Kafka send task
    kafka_task = PythonOperator(
        task_id='send_random_data_to_kafka',
        python_callable=send_random_data_to_kafka
    )
