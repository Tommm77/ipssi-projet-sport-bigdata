#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer, KafkaConsumer
import threading

KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
MATCHS_TOPIC = 'matchs_topic'
USERS_TOPIC = 'users_topic'
NOTIFICATIONS_TOPIC = 'notifications_topic'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Cache local pour stocker les utilisateurs et les matchs
users_cache = {}  # user_id -> user_data
matchs_cache = {}  # match_id -> match_data

# Types de notifications possibles
types_notifications = {
    "programmé": "Un match que vous pourriez aimer a été programmé: {equipe_domicile} vs {equipe_exterieur}",
    "rappel": "Rappel: Le match {equipe_domicile} vs {equipe_exterieur} commence bientôt!",
    "debut_match": "Le match {equipe_domicile} vs {equipe_exterieur} vient de commencer!",
    "mi_temps": "Mi-temps: {equipe_domicile} {score_domicile} - {score_exterieur} {equipe_exterieur}",
    "but": "BUT! {equipe_marqueuse} marque contre {equipe_adverse}! {equipe_domicile} {score_domicile} - {score_exterieur} {equipe_exterieur}",
    "carton": "Carton {type_carton} pour un joueur de {equipe}!",
    "fin_match": "Fin du match: {equipe_domicile} {score_domicile} - {score_exterieur} {equipe_exterieur}"
}

def consommer_users():
    """Consomme les messages du topic users et met à jour le cache local"""
    consumer = KafkaConsumer(
        USERS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',
        enable_auto_commit=True
    )
    
    for message in consumer:
        user_data = message.value
        if 'user_id' in user_data:
            users_cache[user_data['user_id']] = user_data
            print("Utilisateur mis à jour: {}".format(user_data['user_id']))

def consommer_matchs():
    """Consomme les messages du topic matchs et met à jour le cache local"""
    consumer = KafkaConsumer(
        MATCHS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',
        enable_auto_commit=True
    )
    
    for message in consumer:
        match_data = message.value
        if 'match_id' in match_data:
            # Si le match est déjà dans le cache, on le met à jour avec les nouvelles infos
            if match_data['match_id'] in matchs_cache:
                matchs_cache[match_data['match_id']].update(match_data)
            else:
                matchs_cache[match_data['match_id']] = match_data
            
            print("Match mis à jour: {}".format(match_data['match_id']))
            
            # Générer une notification pour ce match
            generer_notification_pour_match(match_data)

def trouver_utilisateurs_interesses(sport_id):
    """Trouve les utilisateurs intéressés par un sport donné"""
    utilisateurs_interesses = []
    for user_id, user_data in users_cache.items():
        if ('sports_favoris' in user_data and 
            sport_id in user_data['sports_favoris'] and
            user_data.get('notification_active', True)):
            utilisateurs_interesses.append(user_id)
    return utilisateurs_interesses

def generer_notification_pour_match(match_data):
    """Génère des notifications pour un match et les envoie aux utilisateurs intéressés"""
    match_id = match_data['match_id']
    
    # Si le match n'a pas de sport_id, on ne peut pas générer de notification
    if 'sport_id' not in match_data:
        return
    
    sport_id = match_data['sport_id']
    statut = match_data.get('statut', 'programmé')
    
    # Trouver les utilisateurs intéressés par ce sport
    utilisateurs_interesses = trouver_utilisateurs_interesses(sport_id)
    
    if not utilisateurs_interesses:
        print("Aucun utilisateur intéressé par le sport {}".format(sport_id))
        return
    
    # Définir le type de notification en fonction du statut
    type_notification = statut  # Par défaut, le type est le statut
    if statut == "en cours" and random.random() < 0.3:
        # Pendant un match, générer aléatoirement d'autres types d'événements
        type_notification = random.choice(["but", "carton", "mi_temps"])
    
    # Préparer les variables pour le template de notification
    template_vars = {
        "equipe_domicile": match_data.get('equipe_domicile', 'Équipe A'),
        "equipe_exterieur": match_data.get('equipe_exterieur', 'Équipe B'),
        "score_domicile": match_data.get('score_domicile', 0),
        "score_exterieur": match_data.get('score_exterieur', 0)
    }
    
    # Ajouter des variables spécifiques pour certains types de notifications
    if type_notification == "but":
        # Déterminer quelle équipe a marqué
        if random.random() < 0.5:
            template_vars["equipe_marqueuse"] = template_vars["equipe_domicile"]
            template_vars["equipe_adverse"] = template_vars["equipe_exterieur"]
        else:
            template_vars["equipe_marqueuse"] = template_vars["equipe_exterieur"]
            template_vars["equipe_adverse"] = template_vars["equipe_domicile"]
    
    if type_notification == "carton":
        template_vars["type_carton"] = random.choice(["jaune", "rouge"])
        template_vars["equipe"] = random.choice([template_vars["equipe_domicile"], template_vars["equipe_exterieur"]])
    
    # Construire le contenu de la notification
    template = types_notifications.get(type_notification, "Mise à jour du match {equipe_domicile} vs {equipe_exterieur}")
    contenu = template.format(**template_vars)
    
    # Envoyer une notification à chaque utilisateur intéressé
    for user_id in utilisateurs_interesses:
        notification = {
            "notification_id": random.randint(10000, 99999),
            "user_id": user_id,
            "match_id": match_id,
            "type": type_notification,
            "contenu": contenu,
            "date_envoi": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "statut": "envoyée"
        }
        
        envoyer_vers_topic(NOTIFICATIONS_TOPIC, notification)

def envoyer_vers_topic(topic, donnees):
    """Envoie des données vers un topic Kafka"""
    future = producer.send(topic, donnees)
    record_metadata = future.get(timeout=10)
    print("Notification envoyee a {}: Partition={}, Offset={}".format(
        topic, record_metadata.partition, record_metadata.offset))

def main():
    # Démarrer les threads de consommation
    thread_users = threading.Thread(target=consommer_users)
    thread_users.daemon = True
    thread_users.start()
    
    thread_matchs = threading.Thread(target=consommer_matchs)
    thread_matchs.daemon = True
    thread_matchs.start()
    
    try:
        print("Moteur de notifications demarre. En attente de matchs et d'utilisateurs...")
        
        # Garder le programme en vie
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("Interruption detectee, arret du producteur de notifications...")
    finally:
        producer.close()
        print("Producteur ferme.")

if __name__ == "__main__":
    main() 