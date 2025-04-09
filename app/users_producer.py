#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
USERS_TOPIC = 'users_topic'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Liste de noms et prénoms pour générer des utilisateurs aléatoires
prenoms = ["Jean", "Marie", "Pierre", "Sophie", "Lucas", "Emma", "Thomas", "Julie", "Nicolas", "Camille",
          "Alexandre", "Laura", "Antoine", "Chloé", "Julien", "Léa", "Maxime", "Sarah", "Paul", "Manon"]

noms = ["Martin", "Bernard", "Dubois", "Thomas", "Robert", "Richard", "Petit", "Durand", "Leroy", "Moreau",
       "Simon", "Laurent", "Lefebvre", "Michel", "Garcia", "David", "Bertrand", "Roux", "Vincent", "Fournier"]

# Fonction pour générer un utilisateur aléatoire
def generer_utilisateur(user_id):
    prenom = random.choice(prenoms)
    nom = random.choice(noms)
    email = "{}.{}@example.com".format(prenom.lower(), nom.lower())
    
    # Date d'inscription dans les 2 dernières années
    date_inscription = datetime.now() - timedelta(days=random.randint(1, 730))
    
    # Sports favoris: entre 1 et 3 sports
    nombre_sports_favoris = random.randint(1, 3)
    sports_favoris = random.sample(range(1, 6), nombre_sports_favoris)
    
    user = {
        "user_id": user_id,
        "prenom": prenom,
        "nom": nom,
        "email": email,
        "date_inscription": date_inscription.strftime("%Y-%m-%d %H:%M:%S"),
        "sports_favoris": sports_favoris,
        "notification_active": random.choice([True, False])
    }
    
    return user

def envoyer_vers_topic(topic, donnees):
    future = producer.send(topic, donnees)
    record_metadata = future.get(timeout=10)
    print("Message envoye a {}: Partition={}, Offset={}".format(
        topic, record_metadata.partition, record_metadata.offset))
    print("Donnees: {}".format(json.dumps(donnees, indent=2)))

def main():
    try:
        # Générer et envoyer des utilisateurs (batch initial)
        for user_id in range(1, 101):  # 100 utilisateurs
            user = generer_utilisateur(user_id)
            envoyer_vers_topic(USERS_TOPIC, user)
            time.sleep(0.5)  # Ralentir légèrement pour ne pas surcharger

        print("Generation initiale d'utilisateurs terminee.")
        
        # Continuer à générer des utilisateurs moins fréquemment
        user_id = 100
        while True:
            user_id += 1
            user = generer_utilisateur(user_id)
            envoyer_vers_topic(USERS_TOPIC, user)
            time.sleep(30)  # Attendre 30 secondes entre chaque nouvel utilisateur
            
    except KeyboardInterrupt:
        print("Interruption detectee, arret du producteur...")
    finally:
        producer.close()
        print("Producteur ferme.")

if __name__ == "__main__":
    main() 