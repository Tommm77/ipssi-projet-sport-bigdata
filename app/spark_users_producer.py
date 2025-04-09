#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType
from pyspark.sql.functions import to_json, struct, lit, array, expr, rand
import time
import json
import random
from datetime import datetime, timedelta

# Configurations
KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
USERS_TOPIC = 'users_topic'

def main():
    # Initialisation d'une session Spark
    spark = SparkSession.builder \
        .appName("Users Producer") \
        .master("local[*]") \
        .getOrCreate()
    
    # Définir le niveau de log pour réduire le bruit
    spark.sparkContext.setLogLevel("WARN")
    
    print("=== Démarrage du producteur d'utilisateurs avec Spark ===")
    
    # Liste de noms et prénoms pour générer des utilisateurs aléatoires
    prenoms = ["Jean", "Marie", "Pierre", "Sophie", "Lucas", "Emma", "Thomas", "Julie", "Nicolas", "Camille",
              "Alexandre", "Laura", "Antoine", "Chloé", "Julien", "Léa", "Maxime", "Sarah", "Paul", "Manon"]
    
    noms = ["Martin", "Bernard", "Dubois", "Thomas", "Robert", "Richard", "Petit", "Durand", "Leroy", "Moreau",
           "Simon", "Laurent", "Lefebvre", "Michel", "Garcia", "David", "Bertrand", "Roux", "Vincent", "Fournier"]
    
    # Créer des DataFrames pour les prénoms et noms
    prenoms_df = spark.createDataFrame([(p,) for p in prenoms], ["prenom"])
    noms_df = spark.createDataFrame([(n,) for n in noms], ["nom"])
    
    # Schéma pour les données utilisateurs
    schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("prenom", StringType(), True),
        StructField("nom", StringType(), True),
        StructField("email", StringType(), True),
        StructField("date_inscription", StringType(), True),
        StructField("sports_favoris", ArrayType(IntegerType()), True),
        StructField("notification_active", BooleanType(), True)
    ])

    try:
        # Générer et envoyer 100 utilisateurs initiaux
        for i in range(1, 101):
            # Sélectionner aléatoirement un prénom et un nom
            prenom = prenoms[random.randint(0, len(prenoms) - 1)]
            nom = noms[random.randint(0, len(noms) - 1)]
            
            # Construire l'email
            email = f"{prenom.lower()}.{nom.lower()}@example.com"
            
            # Générer une date d'inscription aléatoire dans les 2 dernières années
            days_ago = random.randint(1, 730)  # 2 ans en jours
            date_inscription = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
            
            # Sélectionner aléatoirement 1 à 3 sports favoris
            num_sports = random.randint(1, 3)
            sports_favoris = random.sample(range(1, 6), num_sports)  # IDs des sports de 1 à 5
            
            # Notification active ou non
            notification_active = random.choice([True, False])
            
            # Créer l'utilisateur
            user = {
                "user_id": i,
                "prenom": prenom,
                "nom": nom,
                "email": email,
                "date_inscription": date_inscription,
                "sports_favoris": sports_favoris,
                "notification_active": notification_active
            }
            
            # Créer un DataFrame avec l'utilisateur
            user_df = spark.createDataFrame([user], schema)
            
            # Envoyer au topic Kafka
            user_df.selectExpr("to_json(struct(*)) AS value") \
                .write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
                .option("topic", USERS_TOPIC) \
                .save()
            
            print(f"Utilisateur envoyé: {user['prenom']} {user['nom']}")
        
        print("=== Batch initial de 100 utilisateurs envoyé avec succès ===")
        print("=== Envoi de 10 utilisateurs supplémentaires à un rythme plus lent ===")
        
        # Générer et envoyer 10 utilisateurs supplémentaires avec pause
        for i in range(101, 111):
            # Sélectionner aléatoirement un prénom et un nom
            prenom = prenoms[random.randint(0, len(prenoms) - 1)]
            nom = noms[random.randint(0, len(noms) - 1)]
            
            # Construire l'email
            email = f"{prenom.lower()}.{nom.lower()}@example.com"
            
            # Générer une date d'inscription récente
            days_ago = random.randint(1, 30)  # 1 mois en jours
            date_inscription = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
            
            # Sélectionner aléatoirement 1 à 3 sports favoris
            num_sports = random.randint(1, 3)
            sports_favoris = random.sample(range(1, 6), num_sports)  # IDs des sports de 1 à 5
            
            # Notification active ou non
            notification_active = random.choice([True, False])
            
            # Créer l'utilisateur
            user = {
                "user_id": i,
                "prenom": prenom,
                "nom": nom,
                "email": email,
                "date_inscription": date_inscription,
                "sports_favoris": sports_favoris,
                "notification_active": notification_active
            }
            
            # Créer un DataFrame avec l'utilisateur
            user_df = spark.createDataFrame([user], schema)
            
            # Envoyer au topic Kafka
            user_df.selectExpr("to_json(struct(*)) AS value") \
                .write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
                .option("topic", USERS_TOPIC) \
                .save()
            
            print(f"Utilisateur envoyé: {user['prenom']} {user['nom']}")
            time.sleep(10)  # Attendre 10 secondes entre chaque utilisateur
        
    except Exception as e:
        print(f"Erreur lors de la production d'utilisateurs: {str(e)}")
    finally:
        # Arrêter la session Spark
        spark.stop()
        print("=== Session Spark arrêtée ===")

if __name__ == "__main__":
    main() 