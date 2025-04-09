#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import to_json, struct, lit
import time
import json

# Configurations
KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
SPORTS_TOPIC = 'sports_topic'

def main():
    # Initialiser une session Spark
    spark = SparkSession.builder \
        .appName("Sports Producer") \
        .master("local[*]") \
        .getOrCreate()
    
    # Définir le niveau de log pour réduire le bruit
    spark.sparkContext.setLogLevel("WARN")
    
    print("=== Démarrage du producteur de sports avec Spark ===")
    
    # Schéma pour les données de sports
    schema = StructType([
        StructField("sport_id", IntegerType(), True),
        StructField("nom", StringType(), True),
        StructField("categorie", StringType(), True),
        StructField("nombre_joueurs", IntegerType(), True),
        StructField("description", StringType(), True)
    ])
    
    # Données de sports
    sports_data = [
        (1, "Football", "Collectif", 11, "Le football se joue avec un ballon entre deux equipes de 11 joueurs"),
        (2, "Basketball", "Collectif", 5, "Le basketball est un sport ou deux equipes de cinq joueurs s'affrontent pour marquer des paniers"),
        (3, "Tennis", "Individuel", 2, "Le tennis est un sport de raquette qui oppose soit deux joueurs, soit deux equipes de deux joueurs"),
        (4, "Rugby", "Collectif", 15, "Le rugby est un sport collectif de contact qui se joue avec un ballon ovale"),
        (5, "Volleyball", "Collectif", 6, "Le volleyball est un sport ou deux equipes s'affrontent avec un ballon sur un terrain separe par un filet")
    ]
    
    # Créer un DataFrame à partir des données
    sports_df = spark.createDataFrame(sports_data, schema)
    
    # Afficher le DataFrame pour vérification
    print("=== Données de sports à envoyer ===")
    sports_df.show(truncate=False)
    
    try:
        # Envoyer chaque sport au topic Kafka
        for sport in sports_df.collect():
            # Convertir la Row en dictionnaire
            sport_dict = sport.asDict()
            
            # Envoyer au topic Kafka en utilisant Spark Structured Streaming
            df_to_send = spark.createDataFrame([sport_dict], schema)
            
            # Utiliser writeStream pour une approche de streaming
            # ou write.format("kafka") pour une approche batch
            df_to_send.selectExpr("to_json(struct(*)) AS value") \
                .write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
                .option("topic", SPORTS_TOPIC) \
                .save()
            
            print(f"Sport envoyé: {sport_dict['nom']}")
            time.sleep(2)  # Attendre 2 secondes entre chaque envoi
            
        print("=== Tous les sports ont été envoyés avec succès ===")
    
    except Exception as e:
        print(f"Erreur lors de l'envoi des sports: {str(e)}")
    finally:
        # Arrêter la session Spark
        spark.stop()
        print("=== Session Spark arrêtée ===")

if __name__ == "__main__":
    main() 