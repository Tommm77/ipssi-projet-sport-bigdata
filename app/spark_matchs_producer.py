#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType
from pyspark.sql.functions import to_json, struct, lit, current_timestamp, expr, col
import time
import json
import random
from datetime import datetime, timedelta
import requests
import os

# Configurations
KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
MATCHS_TOPIC = 'matchs_topic'
FOOTBALL_API_KEY = os.environ.get('FOOTBALL_API_KEY', '')  # Clé API à définir dans les variables d'environnement
FOOTBALL_API_URL = 'https://api.football-data.org/v4/matches'

def main():
    # Initialiser une session Spark
    spark = SparkSession.builder \
        .appName("Matchs Producer") \
        .master("local[*]") \
        .getOrCreate()
    
    # Définir le niveau de log pour réduire le bruit
    spark.sparkContext.setLogLevel("WARN")
    
    print("=== Démarrage du producteur de matchs avec Spark ===")
    
    # Schéma pour les matchs
    match_schema = StructType([
        StructField("match_id", IntegerType(), True),
        StructField("sport_id", IntegerType(), True),
        StructField("sport_nom", StringType(), True),
        StructField("equipe_domicile", StringType(), True),
        StructField("equipe_exterieur", StringType(), True),
        StructField("score_domicile", IntegerType(), True),
        StructField("score_exterieur", IntegerType(), True),
        StructField("date_match", StringType(), True),
        StructField("lieu", StringType(), True),
        StructField("statut", StringType(), True)
    ])
    
    # Schéma pour les mises à jour de matchs
    update_schema = StructType([
        StructField("match_id", IntegerType(), True),
        StructField("statut", StringType(), True),
        StructField("score_domicile", IntegerType(), True),
        StructField("score_exterieur", IntegerType(), True),
        StructField("derniere_mise_a_jour", StringType(), True)
    ])
    
    # Fonction pour récupérer les matchs depuis l'API
    def recuperer_matchs_api():
        headers = {
            'X-Auth-Token': FOOTBALL_API_KEY
        }
        
        # Récupérer les matchs des 7 prochains jours
        date_debut = datetime.now().strftime("%Y-%m-%d")
        date_fin = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")
        
        params = {
            'dateFrom': date_debut,
            'dateTo': date_fin
        }
        
        try:
            response = requests.get(FOOTBALL_API_URL, headers=headers, params=params)
            response.raise_for_status()  # Lève une exception en cas d'erreur HTTP
            
            data = response.json()
            return data.get('matches', [])
        except Exception as e:
            print(f"Erreur lors de la récupération des matchs: {str(e)}")
            return []
    
    # Fonction pour convertir un match de l'API au format attendu
    def convertir_match_api(match_api, match_id):
        # Déterminer le statut du match
        statut_map = {
            'SCHEDULED': 'programmé',
            'LIVE': 'en cours',
            'FINISHED': 'terminé',
            'POSTPONED': 'reporté',
            'CANCELLED': 'annulé'
        }
        
        # Extraire les scores
        score_domicile = match_api.get('score', {}).get('fullTime', {}).get('home', 0)
        score_exterieur = match_api.get('score', {}).get('fullTime', {}).get('away', 0)
        
        # Convertir la date
        date_match = match_api.get('utcDate', '')
        if date_match:
            try:
                date_obj = datetime.strptime(date_match, "%Y-%m-%dT%H:%M:%SZ")
                date_match = date_obj.strftime("%Y-%m-%d %H:%M:%S")
            except:
                pass
        
        match = {
            "match_id": match_id,
            "sport_id": 1,  # Football
            "sport_nom": "Football",
            "equipe_domicile": match_api.get('homeTeam', {}).get('name', ''),
            "equipe_exterieur": match_api.get('awayTeam', {}).get('name', ''),
            "score_domicile": score_domicile,
            "score_exterieur": score_exterieur,
            "date_match": date_match,
            "lieu": match_api.get('venue', ''),
            "statut": statut_map.get(match_api.get('status', ''), 'programmé')
        }
        
        return match
    
    # Fonction pour générer une mise à jour de match
    def generer_mise_a_jour_match(match_id, match_api):
        statut_map = {
            'SCHEDULED': 'programmé',
            'LIVE': 'en cours',
            'FINISHED': 'terminé',
            'POSTPONED': 'reporté',
            'CANCELLED': 'annulé'
        }
        
        score_domicile = match_api.get('score', {}).get('fullTime', {}).get('home', 0)
        score_exterieur = match_api.get('score', {}).get('fullTime', {}).get('away', 0)
        
        match_update = {
            "match_id": match_id,
            "statut": statut_map.get(match_api.get('status', ''), 'programmé'),
            "score_domicile": score_domicile,
            "score_exterieur": score_exterieur,
            "derniere_mise_a_jour": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        return match_update
    
    try:
        match_id = 1000
        matchs_existants = {}  # Pour stocker les matchs déjà traités
        
        # Boucle principale
        for _ in range(50):  # Limiter à 50 itérations pour éviter une boucle infinie
            match_id += 1
            
            # Récupérer les matchs depuis l'API
            matchs_api = recuperer_matchs_api()
            
            if not matchs_api:
                print("Aucun match trouvé dans l'API")
                time.sleep(60)  # Attendre 1 minute avant de réessayer
                continue
            
            # Traiter chaque match de l'API
            for match_api in matchs_api:
                api_match_id = match_api.get('id')
                
                # Si c'est un nouveau match
                if api_match_id not in matchs_existants:
                    match = convertir_match_api(match_api, match_id)
                    match_df = spark.createDataFrame([match], match_schema)
                    
                    # Envoyer le match au topic Kafka
                    match_df.selectExpr("to_json(struct(*)) AS value") \
                        .write \
                        .format("kafka") \
                        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
                        .option("topic", MATCHS_TOPIC) \
                        .save()
                    
                    print(f"Match envoyé: ID={match['match_id']}, {match['equipe_domicile']} vs {match['equipe_exterieur']}")
                    
                    # Stocker le match pour les mises à jour futures
                    matchs_existants[api_match_id] = {
                        'match_id': match_id,
                        'match_api': match_api
                    }
                    
                    match_id += 1
                else:
                    # Mise à jour d'un match existant
                    match_info = matchs_existants[api_match_id]
                    match_update = generer_mise_a_jour_match(match_info['match_id'], match_api)
                    
                    update_df = spark.createDataFrame([match_update], update_schema)
                    
                    # Envoyer la mise à jour au topic Kafka
                    update_df.selectExpr("to_json(struct(*)) AS value") \
                        .write \
                        .format("kafka") \
                        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
                        .option("topic", MATCHS_TOPIC) \
                        .save()
                    
                    print(f"Mise à jour envoyée pour le match {match_update['match_id']}: statut={match_update['statut']}, score={match_update['score_domicile']}-{match_update['score_exterieur']}")
                    
                    # Mettre à jour les informations du match
                    matchs_existants[api_match_id]['match_api'] = match_api
            
            time.sleep(300)  # Attendre 5 minutes entre chaque requête API
        
        print("=== Producteur de matchs terminé ===")
    
    except Exception as e:
        print(f"Erreur lors de la production de matchs: {str(e)}")
    finally:
        # Arrêter la session Spark
        spark.stop()
        print("=== Session Spark arrêtée ===")

if __name__ == "__main__":
    main()