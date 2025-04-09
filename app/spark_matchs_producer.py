#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from datetime import datetime, timedelta
from pyspark.sql.functions import to_json, struct
import requests
import os

# Configuration Kafka
KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
MATCHS_TOPIC = 'matchs_topic'

# Clé API Football (à définir dans les variables d'environnement)
FOOTBALL_API_KEY = os.environ.get('FOOTBALL_API_KEY', '')
FOOTBALL_API_URL = 'https://api.football-data.org/v4/matches'


def main():
    spark = SparkSession.builder \
        .appName("Matchs Producer") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print("=== Démarrage du producteur de matchs ===")

    # Schéma des matchs
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

    def recuperer_matchs_api():
        headers = {'X-Auth-Token': FOOTBALL_API_KEY}
        date_debut = datetime.now().strftime("%Y-%m-%d")
        date_fin = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")
        params = {'dateFrom': date_debut, 'dateTo': date_fin}

        try:
            response = requests.get(FOOTBALL_API_URL, headers=headers, params=params)
            response.raise_for_status()
            return response.json().get('matches', [])
        except Exception as e:
            print(f"Erreur API : {str(e)}")
            return []

    def convertir_match_api(match_api, match_id):
        # Déterminer le statut du match
        statut_map = {
            'SCHEDULED': 'prévu',
            'TIMED': 'programmé',
            'IN_PLAY': 'en cours',
            'PAUSED': 'en pause',
            'FINISHED': 'terminé',
            'LIVE': 'en cours',
        }
        
        # Extraire les scores
        # S'assurer que les scores ont toujours une valeur par défaut de 0
        score_domicile = 0
        score_exterieur = 0
        
        if match_api.get('score', {}).get('fullTime', {}).get('home') is not None:
            score_domicile = match_api.get('score', {}).get('fullTime', {}).get('home')
        
        if match_api.get('score', {}).get('fullTime', {}).get('away') is not None:
            score_exterieur = match_api.get('score', {}).get('fullTime', {}).get('away')
        
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
        
        # S'assurer que les scores ont toujours une valeur par défaut de 0
        score_domicile = 0
        score_exterieur = 0
        
        if match_api.get('score', {}).get('fullTime', {}).get('home') is not None:
            score_domicile = match_api.get('score', {}).get('fullTime', {}).get('home')
        
        if match_api.get('score', {}).get('fullTime', {}).get('away') is not None:
            score_exterieur = match_api.get('score', {}).get('fullTime', {}).get('away')
        
        match_update = {
            "match_id": match_id,
            "statut": statut_map.get(match_api.get('status', ''), 'programmé'),
            "score_domicile": score_domicile,
            "score_exterieur": score_exterieur,
            "derniere_mise_a_jour": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        return match_update

    try:
        matchs_api = recuperer_matchs_api()
        if not matchs_api:
            print("Aucun match trouvé dans l'API.")
        else:
            match_id = 1000
            for match_api in matchs_api:
                match = convertir_match_api(match_api, match_id)
                match_df = spark.createDataFrame([match], match_schema)

                match_df.selectExpr("to_json(struct(*)) AS value") \
                    .write \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
                    .option("topic", MATCHS_TOPIC) \
                    .save()

                print(f"✔ Match envoyé : ID={match['match_id']} | {match['equipe_domicile']} vs {match['equipe_exterieur']}")
                match_id += 1

        print("=== Tous les matchs ont été envoyés. Fin du script. ===")

    except Exception as e:
        print(f"Erreur pendant l'exécution : {str(e)}")
    finally:
        spark.stop()
        print("=== Session Spark arrêtée ===")


if __name__ == "__main__":
    main()
