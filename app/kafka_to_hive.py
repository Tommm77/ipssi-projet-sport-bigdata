#!/usr/bin/env python
# -*- coding: utf-8 -*-

from kafka import KafkaConsumer
import json
import os
import time
import uuid
import logging
import sys
from datetime import datetime
from hdfs import InsecureClient

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger('kafka-to-hive')

# Configuration Kafka
KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
TOPICS = ['sports_topic', 'matchs_topic', 'users_topic', 'notifications_topic']

# Configuration HDFS
HDFS_URL = 'http://namenode:50070'
HDFS_USER = 'root'
HDFS_BASE_PATH = '/user/hive/warehouse/kafka_data.db'

# Correspondance entre topics et chemins HDFS
TOPIC_TO_PATH = {
    'sports_topic': f"{HDFS_BASE_PATH}/sports",
    'matchs_topic': f"{HDFS_BASE_PATH}/matchs",
    'users_topic': f"{HDFS_BASE_PATH}/users",
    'notifications_topic': f"{HDFS_BASE_PATH}/notifications"
}

def wait_for_services():
    """Attendre que Kafka et HDFS soient disponibles"""
    logger.info("Attente de la disponibilité des services...")
    
    # Attendre que Kafka soit disponible
    max_retries = 30
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
            consumer.close()
            logger.info("Kafka est disponible!")
            break
        except Exception as e:
            retry_count += 1
            logger.warning(f"En attente de Kafka... ({retry_count}/{max_retries})")
            time.sleep(10)
    
    if retry_count >= max_retries:
        logger.error("Impossible de se connecter à Kafka après plusieurs tentatives")
        return False
    
    # Attendre que HDFS soit disponible
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            client = InsecureClient(HDFS_URL, user=HDFS_USER)
            client.list('/')
            logger.info("HDFS est disponible!")
            return True
        except Exception as e:
            retry_count += 1
            logger.warning(f"En attente d'HDFS... ({retry_count}/{max_retries})")
            time.sleep(10)
    
    logger.error("Impossible de se connecter à HDFS après plusieurs tentatives")
    return False

def create_consumer():
    """Crée un consommateur Kafka pour tous les topics"""
    return KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='kafka-to-hive-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

def get_hdfs_client():
    """Crée un client HDFS"""
    return InsecureClient(HDFS_URL, user=HDFS_USER)

def save_to_hdfs(client, topic, data):
    """Sauvegarde les données JSON dans HDFS"""
    path = TOPIC_TO_PATH[topic]
    filename = f"{uuid.uuid4()}.json"
    full_path = f"{path}/{filename}"
    
    # Convertir les données en chaîne JSON
    json_data = json.dumps(data) + '\n'
    
    # Écrire dans HDFS
    with client.write(full_path, encoding='utf-8') as writer:
        writer.write(json_data)
    
    logger.info(f"Données sauvegardées dans {full_path}")

def main():
    """Fonction principale pour consommer des messages Kafka et les sauvegarder dans HDFS"""
    logger.info("Démarrage du processus Kafka -> HDFS -> Hive")
    
    # Attendre que les services soient disponibles
    if not wait_for_services():
        return
    
    # Initialiser le client HDFS
    try:
        hdfs_client = get_hdfs_client()
        logger.info("Connexion HDFS établie")
    except Exception as e:
        logger.error(f"Erreur lors de la connexion à HDFS: {str(e)}")
        return
    
    # Initialiser le consommateur Kafka
    try:
        consumer = create_consumer()
        logger.info("Connexion Kafka établie, en attente de messages...")
    except Exception as e:
        logger.error(f"Erreur lors de la connexion à Kafka: {str(e)}")
        return
    
    # Consommer les messages
    try:
        for message in consumer:
            topic = message.topic
            data = message.value
            
            try:
                save_to_hdfs(hdfs_client, topic, data)
            except Exception as e:
                logger.error(f"Erreur lors de la sauvegarde dans HDFS: {str(e)}")
                # Tenter de se reconnecter à HDFS
                try:
                    logger.info("Tentative de reconnexion à HDFS...")
                    hdfs_client = get_hdfs_client()
                except Exception as e:
                    logger.error(f"Échec de la reconnexion à HDFS: {str(e)}")
                
    except KeyboardInterrupt:
        logger.info("Interruption détectée, arrêt du consommateur...")
    finally:
        consumer.close()
        logger.info("Consommateur fermé.")

if __name__ == "__main__":
    # Attendre que Kafka et HDFS soient disponibles
    time.sleep(60)
    main() 