#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import time
import random
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
SPORTS_TOPIC = 'sports_topic'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Définition des sports
sports = [
    {
        "sport_id": 1, 
        "nom": "Football", 
        "categorie": "Collectif", 
        "nombre_joueurs": 11,
        "description": "Le football se joue avec un ballon entre deux equipes de 11 joueurs"
    },
    {
        "sport_id": 2, 
        "nom": "Basketball", 
        "categorie": "Collectif", 
        "nombre_joueurs": 5,
        "description": "Le basketball est un sport ou deux equipes de cinq joueurs s'affrontent pour marquer des paniers"
    },
    {
        "sport_id": 3, 
        "nom": "Tennis", 
        "categorie": "Individuel", 
        "nombre_joueurs": 2,
        "description": "Le tennis est un sport de raquette qui oppose soit deux joueurs, soit deux equipes de deux joueurs"
    },
    {
        "sport_id": 4, 
        "nom": "Rugby", 
        "categorie": "Collectif", 
        "nombre_joueurs": 15,
        "description": "Le rugby est un sport collectif de contact qui se joue avec un ballon ovale"
    },
    {
        "sport_id": 5, 
        "nom": "Volleyball", 
        "categorie": "Collectif", 
        "nombre_joueurs": 6,
        "description": "Le volleyball est un sport ou deux equipes s'affrontent avec un ballon sur un terrain separe par un filet"
    }
]

def envoyer_vers_topic(topic, donnees):
    future = producer.send(topic, donnees)
    record_metadata = future.get(timeout=10)
    print("Message envoye a {}: Partition={}, Offset={}".format(
        topic, record_metadata.partition, record_metadata.offset))
    print("Donnees: {}".format(json.dumps(donnees, indent=2)))

def main():
    try:
        # Envoyer chaque sport au topic Kafka
        for sport in sports:
            envoyer_vers_topic(SPORTS_TOPIC, sport)
            time.sleep(2)  # Attendre 2 secondes entre chaque envoi

        print("Tous les sports ont ete envoyes avec succes.")
    except KeyboardInterrupt:
        print("Interruption detectee, arret du producteur...")
    finally:
        producer.close()
        print("Producteur ferme.")

if __name__ == "__main__":
    main() 