#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType
from pyspark.sql.functions import col, expr, from_json, to_json, struct, lit, udf, array
import time
import json
import random
from datetime import datetime, timedelta
import threading
from functools import reduce

# Configurations
KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
MATCHS_TOPIC = 'matchs_topic'
USERS_TOPIC = 'users_topic'
NOTIFICATIONS_TOPIC = 'notifications_topic'

def main():
    # Initialiser une session Spark
    spark = SparkSession.builder \
        .appName("Notifications Producer") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()
    
    # Définir le niveau de log pour réduire le bruit
    spark.sparkContext.setLogLevel("WARN")
    
    print("=== Démarrage du producteur de notifications avec Spark ===")
    
    # Schémas pour les données des topics
    users_schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("prenom", StringType(), True),
        StructField("nom", StringType(), True),
        StructField("email", StringType(), True),
        StructField("date_inscription", StringType(), True),
        StructField("sports_favoris", ArrayType(IntegerType()), True),
        StructField("notification_active", BooleanType(), True)
    ])
    
    matchs_schema = StructType([
        StructField("match_id", IntegerType(), True),
        StructField("sport_id", IntegerType(), True),
        StructField("sport_nom", StringType(), True),
        StructField("equipe_domicile", StringType(), True),
        StructField("equipe_exterieur", StringType(), True),
        StructField("score_domicile", IntegerType(), True),
        StructField("score_exterieur", IntegerType(), True),
        StructField("date_match", StringType(), True),
        StructField("lieu", StringType(), True),
        StructField("statut", StringType(), True),
        StructField("derniere_mise_a_jour", StringType(), True)
    ])
    
    notifications_schema = StructType([
        StructField("notification_id", IntegerType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("match_id", IntegerType(), True),
        StructField("type", StringType(), True),
        StructField("contenu", StringType(), True),
        StructField("date_envoi", StringType(), True),
        StructField("statut", StringType(), True)
    ])
    
    # Types de notifications possibles avec leurs templates
    types_notifications = {
        "programmé": "Un match que vous pourriez aimer a été programmé: {equipe_domicile} vs {equipe_exterieur}",
        "rappel": "Rappel: Le match {equipe_domicile} vs {equipe_exterieur} commence bientôt!",
        "debut_match": "Le match {equipe_domicile} vs {equipe_exterieur} vient de commencer!",
        "mi_temps": "Mi-temps: {equipe_domicile} {score_domicile} - {score_exterieur} {equipe_exterieur}",
        "but": "BUT! {equipe_marqueuse} marque contre {equipe_adverse}! {equipe_domicile} {score_domicile} - {score_exterieur} {equipe_exterieur}",
        "carton": "Carton {type_carton} pour un joueur de {equipe}!",
        "fin_match": "Fin du match: {equipe_domicile} {score_domicile} - {score_exterieur} {equipe_exterieur}"
    }
    
    try:
        # 1. Créer un cache local en lisant les données existantes des topics Kafka
        
        # Lire les utilisateurs du topic Kafka
        users_df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", USERS_TOPIC) \
            .option("startingOffsets", "earliest") \
            .load() \
            .selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), users_schema).alias("data")) \
            .select("data.*")
        
        # Lire les matchs du topic Kafka
        matchs_df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", MATCHS_TOPIC) \
            .option("startingOffsets", "earliest") \
            .load() \
            .selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), matchs_schema).alias("data")) \
            .select("data.*")
        
        # Cacher les DataFrames pour une utilisation répétée
        users_df.cache()
        matchs_df.cache()
        
        # Actions pour forcer l'évaluation et le remplissage des caches
        users_count = users_df.count()
        matchs_count = matchs_df.count()
        
        print(f"=== Cache initial rempli avec {users_count} utilisateurs et {matchs_count} matchs ===")
        
        # 2. Démarrer le streaming pour les nouveaux matchs et générer des notifications
        
        # Définir la fonction pour générer des notifications
        def generer_notifications(match_data, users_df):
            match_id = match_data["match_id"]
            sport_id = match_data.get("sport_id")
            
            if not sport_id:
                return []
            
            statut = match_data.get("statut", "programmé")
            
            # Trouver les utilisateurs intéressés par ce sport
            utilisateurs_interesses = users_df \
                .filter(array_contains(col("sports_favoris"), sport_id)) \
                .filter(col("notification_active") == True) \
                .select("user_id")
            
            # Si pas d'utilisateurs intéressés, retourner une liste vide
            if utilisateurs_interesses.count() == 0:
                return []
            
            # Déterminer le type de notification
            type_notification = statut
            if statut == "en cours" and random.random() < 0.3:
                type_notification = random.choice(["but", "carton", "mi_temps"])
            
            # Préparer les variables pour le template
            template_vars = {
                "equipe_domicile": match_data.get("equipe_domicile", "Équipe A"),
                "equipe_exterieur": match_data.get("equipe_exterieur", "Équipe B"),
                "score_domicile": match_data.get("score_domicile", 0),
                "score_exterieur": match_data.get("score_exterieur", 0)
            }
            
            # Ajouter des variables spécifiques pour certains types
            if type_notification == "but":
                if random.random() < 0.5:
                    template_vars["equipe_marqueuse"] = template_vars["equipe_domicile"]
                    template_vars["equipe_adverse"] = template_vars["equipe_exterieur"]
                else:
                    template_vars["equipe_marqueuse"] = template_vars["equipe_exterieur"]
                    template_vars["equipe_adverse"] = template_vars["equipe_domicile"]
            
            if type_notification == "carton":
                template_vars["type_carton"] = random.choice(["jaune", "rouge"])
                template_vars["equipe"] = random.choice([
                    template_vars["equipe_domicile"], 
                    template_vars["equipe_exterieur"]
                ])
            
            # Construire le contenu de la notification
            template = types_notifications.get(
                type_notification, 
                "Mise à jour du match {equipe_domicile} vs {equipe_exterieur}"
            )
            
            # Formater le template avec les variables
            contenu = template.format(**template_vars)
            
            # Générer les notifications pour tous les utilisateurs intéressés
            notifications = []
            for user_row in utilisateurs_interesses.collect():
                user_id = user_row["user_id"]
                
                notification = {
                    "notification_id": random.randint(10000, 99999),
                    "user_id": user_id,
                    "match_id": match_id,
                    "type": type_notification,
                    "contenu": contenu,
                    "date_envoi": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "statut": "envoyée"
                }
                
                notifications.append(notification)
            
            return notifications
        
        # Définir une UDF pour la génération de notifications
        array_contains = lambda arr, val: val in arr
        spark.udf.register("array_contains", array_contains)
        
        # Simuler la consommation de matchs et générer des notifications
        for _ in range(20):  # Limiter à 20 itérations
            # Sélectionner un match aléatoire parmi ceux disponibles
            if matchs_count > 0:
                random_idx = random.randint(0, matchs_count - 1)
                match_row = matchs_df.limit(matchs_count).tail(matchs_count)[random_idx]
                
                # Convertir la Row en dictionnaire
                match_data = {key: match_row[key] for key in match_row.__fields__}
                
                # Générer des notifications pour ce match
                notifications = generer_notifications(match_data, users_df)
                
                # Si des notifications ont été générées, les envoyer à Kafka
                if notifications:
                    notifications_df = spark.createDataFrame(notifications, notifications_schema)
                    
                    # Envoyer les notifications au topic Kafka
                    notifications_df.selectExpr("to_json(struct(*)) AS value") \
                        .write \
                        .format("kafka") \
                        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
                        .option("topic", NOTIFICATIONS_TOPIC) \
                        .save()
                    
                    print(f"=== {len(notifications)} notifications envoyées pour le match {match_data['match_id']} ===")
            
            time.sleep(5)  # Attendre 5 secondes entre chaque génération de notifications
        
        print("=== Producteur de notifications terminé ===")
        
    except Exception as e:
        print(f"Erreur lors de la production de notifications: {str(e)}")
    finally:
        # Arrêter la session Spark
        spark.stop()
        print("=== Session Spark arrêtée ===")

if __name__ == "__main__":
    main() 