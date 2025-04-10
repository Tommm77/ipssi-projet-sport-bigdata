from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from confluent_kafka import Producer
import json
import random
import time

"""
Football Notifications DAG

This DAG is responsible for generating random football match notifications and sending them to the Kafka 'notifications_topic'.
It is configured as the MAIN source of notifications in the system.
The kafka-notifications-producer service has been disabled to ensure only notifications from this DAG are processed.

The notifications are consumed by the backend service and displayed in the frontend application.
"""

# Configuration par défaut du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),  # Modified to allow immediate execution
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Configuration Kafka
KAFKA_CONFIG = {
    'bootstrap.servers': 'kafka:9092',
}
NOTIFICATIONS_TOPIC = 'notifications_topic'

# Équipes de football
EQUIPES_FOOTBALL = ["PSG", "Marseille", "Lyon", "Monaco", "Lille", "Rennes", 
                    "Barcelona", "Real Madrid", "Atletico Madrid", "Manchester United", 
                    "Manchester City", "Liverpool", "Chelsea", "Arsenal", "Bayern Munich"]

# Types de notifications possibles
TYPES_NOTIFICATIONS = {
    "programmé": "Un match que vous pourriez aimer a été programmé: {equipe_domicile} vs {equipe_exterieur}",
    "rappel": "Rappel: Le match {equipe_domicile} vs {equipe_exterieur} commence bientôt!",
    "debut_match": "Le match {equipe_domicile} vs {equipe_exterieur} vient de commencer!",
    "mi_temps": "Mi-temps: {equipe_domicile} {score_domicile} - {score_exterieur} {equipe_exterieur}",
    "but": "BUT! {equipe_marqueuse} marque contre {equipe_adverse}! {equipe_domicile} {score_domicile} - {score_exterieur} {equipe_exterieur}",
    "carton": "Carton {type_carton} pour un joueur de {equipe}!",
    "fin_match": "Fin du match: {equipe_domicile} {score_domicile} - {score_exterieur} {equipe_exterieur}"
}

# Fonction qui envoie une notification de match aléatoire
def send_random_football_notification():
    # Créer un producteur Kafka
    producer = Producer(KAFKA_CONFIG)
    
    # Rapport de livraison pour le message Kafka
    def delivery_report(err, msg):
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")
    
    # Générer deux équipes différentes
    equipe_domicile = random.choice(EQUIPES_FOOTBALL)
    equipe_exterieur = random.choice([e for e in EQUIPES_FOOTBALL if e != equipe_domicile])
    
    # Générer des scores aléatoires
    score_domicile = random.randint(0, 5)
    score_exterieur = random.randint(0, 5)
    
    # Générer un ID de match et utilisateur
    match_id = random.randint(1000, 9999)
    user_id = random.randint(1, 100) 
    
    # Choisir un type de notification aléatoire
    type_notification = random.choice(list(TYPES_NOTIFICATIONS.keys()))
    
    # Préparer les variables pour le template de notification
    template_vars = {
        "equipe_domicile": equipe_domicile,
        "equipe_exterieur": equipe_exterieur,
        "score_domicile": score_domicile,
        "score_exterieur": score_exterieur
    }
    
    # Ajouter des variables spécifiques pour certains types de notifications
    if type_notification == "but":
        # Déterminer quelle équipe a marqué
        if random.random() < 0.5:
            template_vars["equipe_marqueuse"] = template_vars["equipe_domicile"]
            template_vars["equipe_adverse"] = template_vars["equipe_exterieur"]
            template_vars["score_domicile"] += 1  # Incrémenter le score
        else:
            template_vars["equipe_marqueuse"] = template_vars["equipe_exterieur"]
            template_vars["equipe_adverse"] = template_vars["equipe_domicile"]
            template_vars["score_exterieur"] += 1  # Incrémenter le score
    
    if type_notification == "carton":
        template_vars["type_carton"] = random.choice(["jaune", "rouge"])
        template_vars["equipe"] = random.choice([template_vars["equipe_domicile"], template_vars["equipe_exterieur"]])
    
    # Construire le contenu de la notification
    template = TYPES_NOTIFICATIONS.get(type_notification)
    contenu = template.format(**template_vars)
    
    # Créer la notification
    notification = {
        "notification_id": random.randint(10000, 99999),
        "user_id": user_id,
        "match_id": match_id,
        "type": type_notification,
        "contenu": contenu,
        "date_envoi": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "statut": "envoyée"
    }
    
    # Envoyer la notification au topic Kafka
    producer.produce(
        NOTIFICATIONS_TOPIC,
        value=json.dumps(notification).encode('utf-8'),
        callback=delivery_report
    )
    
    # Attendre que tous les messages soient envoyés
    producer.flush()
    
    print(f"Notification envoyée: {notification['contenu']}")
    return notification['contenu']

# Créer le DAG
with DAG(
    'football_notifications',
    default_args=default_args,
    description='Envoie des notifications de matchs de football aléatoires toutes les minutes',
    schedule_interval=timedelta(minutes=1),
    catchup=False,
) as dag:
    
    # Tâche pour envoyer une notification de match
    send_notification_task = PythonOperator(
        task_id='send_football_notification',
        python_callable=send_random_football_notification,
    ) 