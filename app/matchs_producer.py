#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
MATCHS_TOPIC = 'matchs_topic'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

sports = [
    {"sport_id": 1, "nom": "Football"},
    {"sport_id": 2, "nom": "Basketball"},
    {"sport_id": 3, "nom": "Tennis"},
    {"sport_id": 4, "nom": "Rugby"},
    {"sport_id": 5, "nom": "Volleyball"}
]

equipes = {
    1: ["PSG", "Marseille", "Lyon", "Monaco", "Lille", "Rennes"],
    2: ["ASVEL", "Paris Basket", "Monaco", "Nanterre", "Dijon", "Strasbourg"],
    3: ["Joueur1", "Joueur2", "Joueur3", "Joueur4", "Joueur5", "Joueur6"],
    4: ["Toulouse", "La Rochelle", "Bordeaux", "Toulon", "Racing 92", "Clermont"],
    5: ["Paris", "Tours", "Montpellier", "Nantes", "Toulouse", "Cannes"]
}

lieux = {
    1: ["Parc des Princes", "Vélodrome", "Groupama Stadium", "Louis II", "Pierre Mauroy"],
    2: ["Astroballe", "Accor Arena", "Salle Gaston Médecin", "Palais des Sports"],
    3: ["Roland Garros", "Accor Arena", "Court Suzanne-Lenglen", "Court Philippe-Chatrier"],
    4: ["Stade Ernest-Wallon", "Stade Marcel Deflandre", "Stade Chaban-Delmas"],
    5: ["Salle Pierre Coubertin", "Palais des Sports de Tours", "Palais des Sports de Gerland"]
}

def generer_match(match_id):
    sport = random.choice(sports)
    sport_id = sport["sport_id"]
    
    equipes_sport = equipes[sport_id]
    equipe_domicile = random.choice(equipes_sport)
    equipe_exterieur = random.choice([e for e in equipes_sport if e != equipe_domicile])
    
    debut_match = datetime.now() + timedelta(days=random.randint(0, 14), hours=random.randint(0, 23))
    
    match = {
        "match_id": match_id,
        "sport_id": sport_id,
        "sport_nom": sport["nom"],
        "equipe_domicile": equipe_domicile,
        "equipe_exterieur": equipe_exterieur,
        "score_domicile": 0,
        "score_exterieur": 0,
        "date_match": debut_match.strftime("%Y-%m-%d %H:%M:%S"),
        "lieu": random.choice(lieux[sport_id]),
        "statut": "programmé"
    }
    
    return match

def generer_mise_a_jour_match(match_id):
    # Simuler une mise à jour de score ou de statut pour un match existant
    statut = random.choice(["en cours", "terminé"])
    
    match_update = {
        "match_id": match_id,
        "statut": statut,
        "score_domicile": random.randint(0, 5),
        "score_exterieur": random.randint(0, 5),
        "derniere_mise_a_jour": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    return match_update

def envoyer_vers_topic(topic, donnees):
    future = producer.send(topic, donnees)
    record_metadata = future.get(timeout=10)
    print("Message envoye a {}: Partition={}, Offset={}".format(
        topic, record_metadata.partition, record_metadata.offset))
    print("Donnees: {}".format(json.dumps(donnees, indent=2)))

def main():
    try:
        match_id = 1000
        while True:
            match_id += 1
            match = generer_match(match_id)
            envoyer_vers_topic(MATCHS_TOPIC, match)
            
            # Simuler des mises à jour de matchs existants (pour les matchs "en cours")
            if random.random() > 0.7 and match_id > 1010:
                match_update = generer_mise_a_jour_match(random.randint(1000, match_id-1))
                if match_update:
                    envoyer_vers_topic(MATCHS_TOPIC, match_update)
            
            time.sleep(15)  # Attendre 15 secondes entre chaque envoi
    
    except KeyboardInterrupt:
        print("Interruption detectee, arret du producteur...")
    finally:
        producer.close()
        print("Producteur ferme.")

if __name__ == "__main__":
    main() 