#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType
from pyspark.sql.functions import to_json, struct, lit, current_timestamp, expr, col
import time
import json
import random
from datetime import datetime, timedelta

# Configurations
KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
MATCHS_TOPIC = 'matchs_topic'

def main():
    # Initialiser une session Spark
    spark = SparkSession.builder \
        .appName("Matchs Producer") \
        .getOrCreate()
    
    # Définir le niveau de log pour réduire le bruit
    spark.sparkContext.setLogLevel("WARN")
    
    print("=== Démarrage du producteur de matchs avec Spark ===")
    
    # Définition des sports
    sports_data = [
        {"sport_id": 1, "nom": "Football"},
        {"sport_id": 2, "nom": "Basketball"},
        {"sport_id": 3, "nom": "Tennis"},
        {"sport_id": 4, "nom": "Rugby"},
        {"sport_id": 5, "nom": "Volleyball"}
    ]
    
    # Équipes par sport
    equipes = {
        1: ["PSG", "Marseille", "Lyon", "Monaco", "Lille", "Rennes"],
        2: ["ASVEL", "Paris Basket", "Monaco", "Nanterre", "Dijon", "Strasbourg"],
        3: ["Joueur1", "Joueur2", "Joueur3", "Joueur4", "Joueur5", "Joueur6"],
        4: ["Toulouse", "La Rochelle", "Bordeaux", "Toulon", "Racing 92", "Clermont"],
        5: ["Paris", "Tours", "Montpellier", "Nantes", "Toulouse", "Cannes"]
    }
    
    # Lieux par sport
    lieux = {
        1: ["Parc des Princes", "Vélodrome", "Groupama Stadium", "Louis II", "Pierre Mauroy"],
        2: ["Astroballe", "Accor Arena", "Salle Gaston Médecin", "Palais des Sports"],
        3: ["Roland Garros", "Accor Arena", "Court Suzanne-Lenglen", "Court Philippe-Chatrier"],
        4: ["Stade Ernest-Wallon", "Stade Marcel Deflandre", "Stade Chaban-Delmas"],
        5: ["Salle Pierre Coubertin", "Palais des Sports de Tours", "Palais des Sports de Gerland"]
    }
    
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
    
    # Fonction pour générer un match
    def generer_match(match_id):
        sport = random.choice(sports_data)
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
    
    # Fonction pour générer une mise à jour de match
    def generer_mise_a_jour_match(match_id):
        statut = random.choice(["en cours", "terminé"])
        
        match_update = {
            "match_id": match_id,
            "statut": statut,
            "score_domicile": random.randint(0, 5),
            "score_exterieur": random.randint(0, 5),
            "derniere_mise_a_jour": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        return match_update
    
    try:
        match_id = 1000
        
        # Boucle principale
        for _ in range(50):  # Limiter à 50 itérations pour éviter une boucle infinie
            match_id += 1
            
            # Générer un nouveau match
            match = generer_match(match_id)
            match_df = spark.createDataFrame([match], match_schema)
            
            # Envoyer le match au topic Kafka
            match_df.selectExpr("to_json(struct(*)) AS value") \
                .write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
                .option("topic", MATCHS_TOPIC) \
                .save()
            
            print(f"Match envoyé: ID={match['match_id']}, {match['equipe_domicile']} vs {match['equipe_exterieur']}")
            
            # Simuler des mises à jour de matchs existants
            if random.random() > 0.7 and match_id > 1010:
                update_match_id = random.randint(1001, match_id-1)
                match_update = generer_mise_a_jour_match(update_match_id)
                
                update_df = spark.createDataFrame([match_update], update_schema)
                
                # Envoyer la mise à jour au topic Kafka
                update_df.selectExpr("to_json(struct(*)) AS value") \
                    .write \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
                    .option("topic", MATCHS_TOPIC) \
                    .save()
                
                print(f"Mise à jour envoyée pour le match {update_match_id}: statut={match_update['statut']}, score={match_update['score_domicile']}-{match_update['score_exterieur']}")
            
            time.sleep(15)  # Attendre 15 secondes entre chaque envoi
        
        print("=== Producteur de matchs terminé ===")
    
    except Exception as e:
        print(f"Erreur lors de la production de matchs: {str(e)}")
    finally:
        # Arrêter la session Spark
        spark.stop()
        print("=== Session Spark arrêtée ===")

if __name__ == "__main__":
    main() 