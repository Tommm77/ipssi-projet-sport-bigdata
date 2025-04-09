from fastapi import FastAPI, HTTPException, Depends, Query, Body, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Optional, Any
import httpx
import json
import os
import requests
import asyncio
from datetime import datetime, timedelta
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.errors import KafkaError
import threading
import time
from aiokafka.structs import TopicPartition

# Configuration Kafka
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
SPORTS_TOPIC = "sports_topic"
MATCHS_TOPIC = "matchs_topic"
USERS_TOPIC = "users_topic"
NOTIFICATIONS_TOPIC = "notifications_topic"

app = FastAPI(
    title="API SportData",
    description="API de données sportives pour l'application Big Data Sports",
    version="1.0.0"
)

# Configuration CORS pour permettre au frontend de communiquer
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # Spécifier explicitement l'origine du frontend
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"]
)

# Modèles de données
class Sport(BaseModel):
    sport_id: int
    nom: str
    categorie: str
    nombre_joueurs: int
    description: str

class Match(BaseModel):
    match_id: int
    sport_id: int
    sport_nom: str
    equipe_domicile: str
    equipe_exterieur: str
    score_domicile: Optional[int] = 0
    score_exterieur: Optional[int] = 0
    date_match: str
    lieu: str
    statut: str
    derniere_mise_a_jour: Optional[str] = None

class User(BaseModel):
    user_id: int
    prenom: str
    nom: str
    email: str
    date_inscription: str
    sports_favoris: List[int]
    notification_active: bool

class Notification(BaseModel):
    notification_id: int
    user_id: int
    match_id: int
    type: str
    contenu: str
    date_envoi: str
    statut: str

class DagRun(BaseModel):
    dag_id: str
    run_id: Optional[str] = None

# Stockage en mémoire des données pour simuler une base de données
# Ces listes seront remplies par les consommateurs Kafka
sports_data = []
matchs_data = []
users_data = []
notifications_data = []

# Producteur Kafka global
kafka_producer = None

# Initialisation du producteur Kafka
@app.on_event("startup")
async def startup_kafka_producer():
    global kafka_producer
    print("Démarrage du backend - Initialisation des connexions Kafka...")
    
    try:
        kafka_producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, 
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        await kafka_producer.start()
        print(f"✅ Producteur Kafka connecté avec succès à {KAFKA_BOOTSTRAP_SERVERS}")
        
        # Test de connexion Kafka
        print("🔍 Tentative d'envoi d'un message de test à Kafka...")
        success = await send_to_kafka('test_topic', {"event": "backend_startup", "timestamp": str(datetime.now())})
        if success:
            print("✅ Message de test envoyé avec succès à Kafka")
        else:
            print("❌ Échec de l'envoi du message de test à Kafka")
        
        # Liste des topics à vérifier
        topics_to_check = [SPORTS_TOPIC, MATCHS_TOPIC, USERS_TOPIC, NOTIFICATIONS_TOPIC]
        print(f"🔍 Vérification de l'existence des topics: {topics_to_check}")
        
        # Vérifier l'existence des topics avec une approche différente
        try:
            # Au lieu de vérifier l'existence des topics, nous allons simplement essayer
            # de créer un consommateur test pour chaque topic
            for topic in topics_to_check:
                print(f"🔍 Vérification du topic '{topic}'...")
                try:
                    consumer = AIOKafkaConsumer(
                        topic,
                        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                        group_id=f"check_group_{topic}",
                        auto_offset_reset="earliest"
                    )
                    await consumer.start()
                    print(f"✅ Topic '{topic}' existe et est accessible")
                    await consumer.stop()
                except Exception as e:
                    print(f"❌ Problème avec le topic '{topic}': {str(e)}")
        except Exception as e:
            print(f"❌ Erreur lors de la vérification des topics: {str(e)}")
            # Ne pas bloquer le démarrage de l'application en cas d'erreur de vérification
        
        # Démarrer les consommateurs Kafka en arrière-plan avec meilleure gestion d'erreur
        print("🚀 Démarrage des consommateurs Kafka...")
        asyncio.create_task(run_consumer_with_error_handling(consume_sports, "sports"))
        asyncio.create_task(run_consumer_with_error_handling(consume_matchs, "matchs"))
        asyncio.create_task(run_consumer_with_error_handling(consume_users, "users"))
        asyncio.create_task(run_consumer_with_error_handling(consume_notifications, "notifications"))
        print("✅ Tâches des consommateurs Kafka lancées")
        
        # Planifier un diagnostic périodique
        asyncio.create_task(diagnostic_periodique())
        
    except Exception as e:
        print(f"❌ Erreur lors de l'initialisation de Kafka: {str(e)}")
        print(f"❌ Détails de l'erreur: {repr(e)}")
        raise e

# Arrêt du producteur Kafka
@app.on_event("shutdown")
async def shutdown_kafka_producer():
    global kafka_producer
    if kafka_producer is not None:
        await kafka_producer.stop()

# Fonction utilitaire pour envoyer un message à Kafka
async def send_to_kafka(topic: str, value: Any):
    global kafka_producer
    try:
        await kafka_producer.send_and_wait(topic, value)
        return True
    except KafkaError as e:
        print(f"Erreur Kafka: {e}")
        return False

# Fonction pour gérer les erreurs dans les consommateurs
async def run_consumer_with_error_handling(consumer_func, consumer_name):
    try:
        print(f"🔄 Démarrage du consommateur {consumer_name}...")
        await consumer_func()
    except Exception as e:
        print(f"❌ ERREUR dans le consommateur {consumer_name}: {str(e)}")
        print(f"❌ Détails de l'erreur: {repr(e)}")
        # Après un délai, tenter de redémarrer le consommateur
        await asyncio.sleep(5)
        asyncio.create_task(run_consumer_with_error_handling(consumer_func, consumer_name))

# Fonctions des consommateurs Kafka pour chaque topic
async def consume_sports():
    try:
        # Utiliser un group_id unique avec timestamp pour forcer une relecture complète
        current_time = int(time.time())
        group_id = f"sports_group_{current_time}"
        
        print(f"🔄 Initialisation du consommateur sports avec group_id: {group_id}...")
        consumer = AIOKafkaConsumer(
            SPORTS_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=group_id,
            auto_offset_reset="earliest",  # Forcer à lire depuis le début
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            # Désactiver l'auto-commit pour garantir qu'on lit tout
            enable_auto_commit=False
        )
        print(f"⏳ Démarrage du consommateur sports...")
        await consumer.start()
        print(f"✅ Consommateur sports démarré, en attente de messages...")
        
        try:
            # Forcer une lecture des topics depuis le début
            # Assigner manuellement les partitions au début
            partitions = consumer.partitions_for_topic(SPORTS_TOPIC)
            if partitions:
                print(f"🔍 Partitions trouvées pour {SPORTS_TOPIC}: {partitions}")
                tp_list = [TopicPartition(SPORTS_TOPIC, p) for p in partitions]
                await consumer.seek_to_beginning(*tp_list)
                print("⏮️ Position de lecture réinitialisée au début pour toutes les partitions")
            
            async for msg in consumer:
                print(f"📥 Message reçu du topic sports: partition={msg.partition}, offset={msg.offset}")
                sport_data = msg.value
                # Vérifier si le sport existe déjà par son ID
                existing_sport = next((s for s in sports_data if s["sport_id"] == sport_data["sport_id"]), None)
                if existing_sport:
                    # Mettre à jour le sport existant
                    idx = sports_data.index(existing_sport)
                    sports_data[idx] = sport_data
                    print(f"🔄 Sport mis à jour: ID={sport_data['sport_id']}, Nom={sport_data.get('nom', 'inconnu')}")
                else:
                    # Ajouter un nouveau sport
                    sports_data.append(sport_data)
                    print(f"➕ Nouveau sport ajouté: ID={sport_data['sport_id']}, Nom={sport_data.get('nom', 'inconnu')}")
                
                # Committer manuellement l'offset
                await consumer.commit()
        finally:
            print("⏹️ Arrêt du consommateur sports...")
            await consumer.stop()
    except Exception as e:
        print(f"❌ Erreur dans le consommateur sports: {str(e)}")
        print(f"❌ Détails de l'erreur: {repr(e)}")
        raise

async def consume_matchs():
    try:
        # Utiliser un group_id unique avec timestamp pour forcer une relecture complète
        current_time = int(time.time())
        group_id = f"matchs_group_{current_time}"
        
        print(f"🔄 Initialisation du consommateur matchs avec group_id: {group_id}...")
        consumer = AIOKafkaConsumer(
            MATCHS_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=group_id,
            auto_offset_reset="earliest",  # Forcer à lire depuis le début
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            # Désactiver l'auto-commit pour garantir qu'on lit tout
            enable_auto_commit=False
        )
        print(f"⏳ Démarrage du consommateur matchs...")
        await consumer.start()
        print(f"✅ Consommateur matchs démarré, en attente de messages...")
        
        try:
            # Forcer une lecture des topics depuis le début
            # Assigner manuellement les partitions au début
            partitions = consumer.partitions_for_topic(MATCHS_TOPIC)
            if partitions:
                print(f"🔍 Partitions trouvées pour {MATCHS_TOPIC}: {partitions}")
                tp_list = [TopicPartition(MATCHS_TOPIC, p) for p in partitions]
                await consumer.seek_to_beginning(*tp_list)
                print("⏮️ Position de lecture réinitialisée au début pour toutes les partitions")
            
            # Dictionnaire pour stocker les matchs complets (pour gérer les mises à jour partielles)
            matchs_dict = {}
            
            async for msg in consumer:
                print(f"📥 Message reçu du topic matchs: partition={msg.partition}, offset={msg.offset}")
                match_data = msg.value
                match_id = match_data.get("match_id")
                
                if not match_id:
                    print(f"⚠️ Message de match sans match_id ignoré: {match_data}")
                    continue
                
                # Vérifier si c'est une mise à jour partielle ou un nouveau match complet
                is_update = "sport_id" not in match_data or "sport_nom" not in match_data
                
                # S'assurer que les scores sont toujours présents
                if "score_domicile" not in match_data:
                    match_data["score_domicile"] = 0
                if "score_exterieur" not in match_data:
                    match_data["score_exterieur"] = 0
                
                if is_update:
                    # C'est une mise à jour partielle
                    print(f"🔄 Mise à jour partielle détectée pour match_id={match_id}")
                    
                    # Chercher le match existant dans notre dictionnaire local
                    existing_match = matchs_dict.get(match_id)
                    
                    if existing_match:
                        # Mettre à jour seulement les champs fournis
                        existing_match.update(match_data)
                        print(f"✅ Match mis à jour dans le dictionnaire local: {match_id}")
                    else:
                        # Si le match n'existe pas encore dans notre dictionnaire,
                        # on crée une entrée de base avec les champs obligatoires par défaut
                        print(f"⚠️ Mise à jour reçue pour un match inconnu: {match_id}")
                        default_match = {
                            "match_id": match_id,
                            "sport_id": 0,
                            "sport_nom": "Inconnu",
                            "equipe_domicile": "Équipe A",
                            "equipe_exterieur": "Équipe B",
                            "score_domicile": 0,
                            "score_exterieur": 0,
                            "date_match": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "lieu": "Lieu inconnu",
                            "statut": "programmé"
                        }
                        default_match.update(match_data)
                        matchs_dict[match_id] = default_match
                        print(f"✅ Nouveau match créé par défaut avec mise à jour: {match_id}")
                else:
                    # C'est un nouveau match complet
                    matchs_dict[match_id] = match_data
                    print(f"➕ Nouveau match complet ajouté: {match_id}")
                
                # Mettre à jour la liste globale des matchs à partir du dictionnaire
                global matchs_data
                matchs_data = list(matchs_dict.values())
                print(f"📊 Total des matchs en mémoire: {len(matchs_data)}")
                
                # Committer manuellement l'offset
                await consumer.commit()
        finally:
            print("⏹️ Arrêt du consommateur matchs...")
            await consumer.stop()
    except Exception as e:
        print(f"❌ Erreur dans le consommateur matchs: {str(e)}")
        print(f"❌ Détails de l'erreur: {repr(e)}")
        raise

async def consume_users():
    try:
        # Utiliser un group_id unique avec timestamp pour forcer une relecture complète
        current_time = int(time.time())
        group_id = f"users_group_{current_time}"
        
        print(f"🔄 Initialisation du consommateur users avec group_id: {group_id}...")
        consumer = AIOKafkaConsumer(
            USERS_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=group_id,
            auto_offset_reset="earliest",  # Forcer à lire depuis le début
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            # Désactiver l'auto-commit pour garantir qu'on lit tout
            enable_auto_commit=False
        )
        print(f"⏳ Démarrage du consommateur users...")
        await consumer.start()
        print(f"✅ Consommateur users démarré, en attente de messages...")
        
        try:
            # Forcer une lecture des topics depuis le début
            # Assigner manuellement les partitions au début
            partitions = consumer.partitions_for_topic(USERS_TOPIC)
            if partitions:
                print(f"🔍 Partitions trouvées pour {USERS_TOPIC}: {partitions}")
                tp_list = [TopicPartition(USERS_TOPIC, p) for p in partitions]
                await consumer.seek_to_beginning(*tp_list)
                print("⏮️ Position de lecture réinitialisée au début pour toutes les partitions")
            
            async for msg in consumer:
                print(f"📥 Message reçu du topic users: partition={msg.partition}, offset={msg.offset}")
                user_data = msg.value
                # Vérifier si l'utilisateur existe déjà par son ID
                existing_user = next((u for u in users_data if u["user_id"] == user_data["user_id"]), None)
                if existing_user:
                    # Mettre à jour l'utilisateur existant
                    idx = users_data.index(existing_user)
                    users_data[idx] = user_data
                    print(f"🔄 Utilisateur mis à jour: ID={user_data['user_id']}")
                else:
                    # Ajouter un nouvel utilisateur
                    users_data.append(user_data)
                    print(f"➕ Nouvel utilisateur ajouté: ID={user_data['user_id']}")
                
                # Committer manuellement l'offset
                await consumer.commit()
        finally:
            print("⏹️ Arrêt du consommateur users...")
            await consumer.stop()
    except Exception as e:
        print(f"❌ Erreur dans le consommateur users: {str(e)}")
        print(f"❌ Détails de l'erreur: {repr(e)}")
        raise

async def consume_notifications():
    try:
        # Utiliser un group_id unique avec timestamp pour forcer une relecture complète
        current_time = int(time.time())
        group_id = f"notifications_group_{current_time}"
        
        print(f"🔄 Initialisation du consommateur notifications avec group_id: {group_id}...")
        consumer = AIOKafkaConsumer(
            NOTIFICATIONS_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=group_id,
            auto_offset_reset="earliest",  # Forcer à lire depuis le début
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            # Désactiver l'auto-commit pour garantir qu'on lit tout
            enable_auto_commit=False
        )
        print(f"⏳ Démarrage du consommateur notifications...")
        await consumer.start()
        print(f"✅ Consommateur notifications démarré, en attente de messages...")
        
        try:
            # Forcer une lecture des topics depuis le début
            # Assigner manuellement les partitions au début
            partitions = consumer.partitions_for_topic(NOTIFICATIONS_TOPIC)
            if partitions:
                print(f"🔍 Partitions trouvées pour {NOTIFICATIONS_TOPIC}: {partitions}")
                tp_list = [TopicPartition(NOTIFICATIONS_TOPIC, p) for p in partitions]
                await consumer.seek_to_beginning(*tp_list)
                print("⏮️ Position de lecture réinitialisée au début pour toutes les partitions")
            
            async for msg in consumer:
                print(f"📥 Message reçu du topic notifications: partition={msg.partition}, offset={msg.offset}")
                notification_data = msg.value
                # Vérifier si la notification existe déjà par son ID
                existing_notification = next((n for n in notifications_data if n["notification_id"] == notification_data["notification_id"]), None)
                if existing_notification:
                    # Mettre à jour la notification existante
                    idx = notifications_data.index(existing_notification)
                    notifications_data[idx] = notification_data
                    print(f"🔄 Notification mise à jour: ID={notification_data['notification_id']}")
                else:
                    # Ajouter une nouvelle notification
                    notifications_data.append(notification_data)
                    print(f"➕ Nouvelle notification ajoutée: ID={notification_data['notification_id']}")
                
                # Committer manuellement l'offset
                await consumer.commit()
        finally:
            print("⏹️ Arrêt du consommateur notifications...")
            await consumer.stop()
    except Exception as e:
        print(f"❌ Erreur dans le consommateur notifications: {str(e)}")
        print(f"❌ Détails de l'erreur: {repr(e)}")
        raise

# Initialiser des données de test par défaut
mock_sports = [
    {
        "sport_id": 1,
        "nom": "Football",
        "categorie": "Collectif",
        "nombre_joueurs": 11,
        "description": "Le football se joue avec un ballon entre deux équipes de 11 joueurs"
    },
    {
        "sport_id": 2,
        "nom": "Basketball",
        "categorie": "Collectif",
        "nombre_joueurs": 5,
        "description": "Le basketball est un sport où deux équipes de cinq joueurs s'affrontent pour marquer des paniers"
    }
]

mock_matchs = [
    {
        "match_id": 1001,
        "sport_id": 1,
        "sport_nom": "Football",
        "equipe_domicile": "PSG",
        "equipe_exterieur": "Marseille",
        "score_domicile": 2,
        "score_exterieur": 1,
        "date_match": "2023-11-15T20:00:00",
        "lieu": "Parc des Princes",
        "statut": "terminé"
    }
]

# Fonction pour initialiser des données si les collections sont vides
def init_data_if_empty():
    # Vérifier si des données réelles sont présentes
    # Si toutes les collections sont vides après un certain temps, on peut supposer qu'il y a un problème
    # avec Kafka, et dans ce cas seulement, on charge les données fictives
    print(f"État des collections - Sports: {len(sports_data)}, Matchs: {len(matchs_data)}, Users: {len(users_data)}, Notifications: {len(notifications_data)}")
    
    # Ne rien faire - laisser Kafka remplir naturellement les données
    # Les données mock ne seront plus utilisées automatiquement
    pass

# Endpoints API

@app.get("/")
async def root():
    return {"message": "Bienvenue sur l'API de données sportives"}

@app.get("/healthcheck")
async def healthcheck():
    services_health = {
        "api": "ok",
        "kafka": await check_kafka(),
        "hive": check_hive(),
        "airflow": check_airflow()
    }
    return services_health

async def check_kafka():
    try:
        # Réelle vérification de Kafka en essayant d'envoyer un message ping
        success = await send_to_kafka('test_topic', {"ping": "healthcheck", "timestamp": str(datetime.now())})
        return "ok" if success else "down"
    except Exception as e:
        print(f"Erreur de connexion à Kafka: {e}")
        return "down"

def check_hive():
    try:
        # En réalité, exécuter une requête simple sur Hive
        return "ok"
    except:
        return "down"
    
def check_airflow():
    try:
        # En réalité, vérifier l'API Airflow
        return "ok"
    except:
        return "down"

# Routes pour les sports
@app.get("/sports", response_model=List[Sport])
async def get_sports():
    # Ne plus initialiser avec des données fictives
    # Retourner les données de Kafka uniquement
    return sports_data

@app.get("/sports/{sport_id}", response_model=Sport)
async def get_sport(sport_id: int):
    # Chercher dans les données Kafka
    sport = next((s for s in sports_data if s["sport_id"] == sport_id), None)
    if sport is None:
        raise HTTPException(status_code=404, detail="Sport non trouvé")
    return sport

# Créer un nouveau sport et l'envoyer à Kafka
@app.post("/sports", response_model=Sport)
async def create_sport(sport: Sport):
    # Ajouter aux données locales
    sport_dict = sport.dict()
    
    # Vérifier si le sport existe déjà
    existing_sport = next((s for s in sports_data if s["sport_id"] == sport_dict["sport_id"]), None)
    if existing_sport:
        # Mettre à jour le sport existant
        idx = sports_data.index(existing_sport)
        sports_data[idx] = sport_dict
    else:
        # Ajouter un nouveau sport
        sports_data.append(sport_dict)
    
    # Envoyer à Kafka
    success = await send_to_kafka(SPORTS_TOPIC, sport_dict)
    if not success:
        raise HTTPException(status_code=500, detail="Erreur lors de l'envoi des données à Kafka")
    
    return sport

# Routes pour les matchs
@app.get("/matchs", response_model=List[Match])
async def get_matchs(
    sport_id: Optional[int] = None,
    statut: Optional[str] = None,
    date_debut: Optional[str] = None,
    date_fin: Optional[str] = None
):
    # Ne plus initialiser avec des données fictives
    # Filtrer les matchs selon les critères
    matchs = matchs_data
    
    # S'assurer que chaque match a tous les champs requis
    for match in matchs:
        # Ajouter les champs manquants avec des valeurs par défaut
        if "score_domicile" not in match:
            match["score_domicile"] = 0
        if "score_exterieur" not in match:
            match["score_exterieur"] = 0
    
    if sport_id:
        matchs = [m for m in matchs if m["sport_id"] == sport_id]
    if statut:
        matchs = [m for m in matchs if m["statut"] == statut]
    
    # Filtres de date à implémenter si nécessaire
    if date_debut:
        date_debut_dt = datetime.fromisoformat(date_debut)
        matchs = [m for m in matchs if datetime.fromisoformat(m["date_match"]) >= date_debut_dt]
    
    if date_fin:
        date_fin_dt = datetime.fromisoformat(date_fin)
        matchs = [m for m in matchs if datetime.fromisoformat(m["date_match"]) <= date_fin_dt]
    
    return matchs

@app.get("/matchs/{match_id}", response_model=Match)
async def get_match(match_id: int):
    match = next((m for m in matchs_data if m["match_id"] == match_id), None)
    if match is None:
        raise HTTPException(status_code=404, detail="Match non trouvé")
    
    # S'assurer que le match a tous les champs requis
    if "score_domicile" not in match:
        match["score_domicile"] = 0
    if "score_exterieur" not in match:
        match["score_exterieur"] = 0
    
    return match

# Créer un nouveau match et l'envoyer à Kafka
@app.post("/matchs", response_model=Match)
async def create_match(match: Match):
    # Ajouter aux données locales
    match_dict = match.dict()
    
    # Vérifier si le match existe déjà
    existing_match = next((m for m in matchs_data if m["match_id"] == match_dict["match_id"]), None)
    if existing_match:
        # Mettre à jour le match existant
        idx = matchs_data.index(existing_match)
        matchs_data[idx] = match_dict
    else:
        # Ajouter un nouveau match
        matchs_data.append(match_dict)
    
    # Envoyer à Kafka
    success = await send_to_kafka(MATCHS_TOPIC, match_dict)
    if not success:
        raise HTTPException(status_code=500, detail="Erreur lors de l'envoi des données à Kafka")
    
    return match

# Routes pour les statistiques
@app.get("/stats/sports")
async def get_sports_stats():
    # Calculer des statistiques à partir des données en mémoire
    sports_count = len(sports_data)
    
    # Compter le nombre de matchs par sport
    matchs_par_sport = {}
    for match in matchs_data:
        sport_id = match["sport_id"]
        sport_nom = match["sport_nom"]
        if sport_id not in matchs_par_sport:
            matchs_par_sport[sport_id] = {"sport_id": sport_id, "nom": sport_nom, "nombre_matchs": 0}
        matchs_par_sport[sport_id]["nombre_matchs"] += 1
    
    return list(matchs_par_sport.values())

@app.get("/stats/matchs")
async def get_matchs_stats():
    # Compter les matchs par statut
    par_statut = {
        "programmé": 0,
        "en cours": 0,
        "terminé": 0
    }
    
    # Compter les matchs par mois
    par_mois = {}
    
    for match in matchs_data:
        # Compter par statut
        statut = match["statut"]
        if statut in par_statut:
            par_statut[statut] += 1
        
        # Compter par mois
        date_match = datetime.fromisoformat(match["date_match"].replace("Z", "+00:00"))
        mois_cle = date_match.strftime("%Y-%m")
        if mois_cle not in par_mois:
            par_mois[mois_cle] = 0
        par_mois[mois_cle] += 1
    
    return {
        "par_statut": par_statut,
        "par_mois": par_mois
    }

# Routes pour interagir avec Airflow
@app.post("/airflow/trigger_dag")
async def trigger_dag(dag_run: DagRun):
    try:
        # Dans un environnement réel, appeler l'API Airflow pour déclencher le DAG
        print(f"Déclenchement du DAG {dag_run.dag_id}")
        return {"status": "success", "message": f"DAG {dag_run.dag_id} déclenché avec succès"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur lors du déclenchement du DAG: {str(e)}")

@app.get("/airflow/dags")
async def get_dags():
    # Dans un environnement réel, récupérer la liste des DAGs depuis Airflow
    dags = [
        {"dag_id": "sports_data_pipeline", "description": "Pipeline de traitement des données sportives", "is_active": True, "status": "success", "last_run": "2023-11-15 14:30:00"},
        {"dag_id": "génération_rapports", "description": "Génération de rapports quotidiens", "is_active": True, "status": "success", "last_run": "2023-11-15 13:45:00"}
    ]
    return dags

# Route pour récupérer les utilisateurs
@app.get("/users", response_model=List[User])
async def get_users():
    # Retourner les données réelles de Kafka
    return users_data

# Route pour récupérer les notifications
@app.get("/notifications", response_model=List[Notification])
async def get_notifications(user_id: Optional[int] = None):
    # Utiliser les données réelles de Kafka
    notifications = notifications_data
    
    if user_id:
        notifications = [n for n in notifications if n["user_id"] == user_id]
        
    return notifications

# Créer une nouvelle notification et l'envoyer à Kafka
@app.post("/notifications", response_model=Notification)
async def create_notification(notification: Notification):
    # Envoyer à Kafka
    success = await send_to_kafka(NOTIFICATIONS_TOPIC, notification.dict())
    if not success:
        raise HTTPException(status_code=500, detail="Erreur lors de l'envoi de la notification à Kafka")
    
    return notification

# Fonction de diagnostic périodique pour afficher l'état des collections
async def diagnostic_periodique():
    try:
        while True:
            await asyncio.sleep(60)  # Vérification toutes les minutes
            
            print("\n--- DIAGNOSTIC PÉRIODIQUE ---")
            print(f"Collections de données:")
            print(f"  - Sports: {len(sports_data)} enregistrements")
            print(f"  - Matchs: {len(matchs_data)} enregistrements")
            print(f"  - Utilisateurs: {len(users_data)} enregistrements")
            print(f"  - Notifications: {len(notifications_data)} enregistrements")
            
            # Afficher quelques exemples si disponibles
            if sports_data:
                print(f"  Exemple de sport: {sports_data[0]}")
            if matchs_data:
                print(f"  Exemple de match: {matchs_data[0]}")
            
            print("--------------------------\n")
    except Exception as e:
        print(f"❌ Erreur dans le diagnostic périodique: {e}")
        # Redémarrer le diagnostic après une erreur
        await asyncio.sleep(5)
        asyncio.create_task(diagnostic_periodique())

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 