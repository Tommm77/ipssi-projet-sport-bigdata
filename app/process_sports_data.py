#!/usr/bin/env python3
# Script Spark pour traiter les données des sports, matchs et notifications

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, expr, lit, current_timestamp, array_contains, collect_list
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, ArrayType, BooleanType

def main():
    # Créer une session Spark avec support Hive
    spark = SparkSession.builder \
        .appName("SportDataProcessor") \
        .enableHiveSupport() \
        .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()
    
    # Réduire le niveau de log pour une meilleure lisibilité
    spark.sparkContext.setLogLevel("WARN")
    
    # Schéma des données des matchs
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
        StructField("statut", StringType(), True),
        StructField("derniere_mise_a_jour", StringType(), True)
    ])
    
    # Schéma des données des sports
    sport_schema = StructType([
        StructField("sport_id", IntegerType(), True),
        StructField("nom", StringType(), True),
        StructField("categorie", StringType(), True),
        StructField("nombre_joueurs", IntegerType(), True),
        StructField("description", StringType(), True)
    ])
    
    # Schéma des données utilisateurs
    user_schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("prenom", StringType(), True),
        StructField("nom", StringType(), True),
        StructField("email", StringType(), True),
        StructField("date_inscription", StringType(), True),
        StructField("sports_favoris", ArrayType(IntegerType()), True),
        StructField("notification_active", BooleanType(), True)
    ])
    
    # Schéma des données notifications
    notification_schema = StructType([
        StructField("notification_id", IntegerType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("match_id", IntegerType(), True),
        StructField("type", StringType(), True),
        StructField("contenu", StringType(), True),
        StructField("date_envoi", StringType(), True),
        StructField("statut", StringType(), True)
    ])
    
    # Configurer les flux de lecture Kafka
    df_matchs = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "matchs_topic") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    df_sports = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "sports_topic") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    df_users = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "users_topic") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    df_notifications = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "notifications_topic") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Convertir les valeurs binaires en chaînes
    json_df_matchs = df_matchs.selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value")
    json_df_sports = df_sports.selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value")
    json_df_users = df_users.selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value")
    json_df_notifications = df_notifications.selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value")
    
    # TRAITEMENT DES DONNÉES DES MATCHS
    # ---------------------------------
    def process_matchs_batch(batch_df, batch_id):
        try:
            if batch_df.isEmpty():
                print(f"\n--- BATCH {batch_id} MATCHS VIDE ---")
                return
                
            print(f"\n--- DONNÉES BRUTES MATCHS (BATCH {batch_id}) ---")
            
            # Parser les données JSON
            parsed_df = batch_df.select(
                col("raw_data"),
                from_json(col("raw_data"), match_schema).alias("data")
            )
            
            # Extraire les champs
            matchs_df = parsed_df.select(
                col("data.match_id").alias("match_id"),
                col("data.sport_id").alias("sport_id"),
                col("data.sport_nom").alias("sport_nom"),
                col("data.equipe_domicile").alias("equipe_domicile"),
                col("data.equipe_exterieur").alias("equipe_exterieur"),
                col("data.score_domicile").alias("score_domicile"),
                col("data.score_exterieur").alias("score_exterieur"),
                col("data.date_match").cast(TimestampType()).alias("date_match"),
                col("data.lieu").alias("lieu"),
                col("data.statut").alias("statut"),
                col("data.derniere_mise_a_jour").cast(TimestampType()).alias("derniere_mise_a_jour"),
                current_timestamp().alias("processed_time")
            )
            
            # Filtrer pour ne garder que les matchs valides
            valid_df = matchs_df.filter(col("match_id").isNotNull())
            
            if valid_df.count() > 0:
                print(f"--- {valid_df.count()} MATCHS VALIDES ---")
                
                # Créer la table Hive si elle n'existe pas
                spark.sql("""
                    CREATE TABLE IF NOT EXISTS default.matchs (
                        match_id INT,
                        sport_id INT,
                        sport_nom STRING,
                        equipe_domicile STRING,
                        equipe_exterieur STRING,
                        score_domicile INT,
                        score_exterieur INT,
                        date_match TIMESTAMP,
                        lieu STRING,
                        statut STRING,
                        derniere_mise_a_jour TIMESTAMP,
                        processed_time TIMESTAMP
                    )
                    USING hive
                    PARTITIONED BY (year INT, month INT, day INT)
                """)
                
                # Ajouter les partitions temporelles
                partitioned_df = valid_df.withColumn("year", expr("year(date_match)")) \
                                         .withColumn("month", expr("month(date_match)")) \
                                         .withColumn("day", expr("dayofmonth(date_match)"))
                
                # Sauvegarder dans Hive
                partitioned_df.write \
                    .format("hive") \
                    .mode("append") \
                    .partitionBy("year", "month", "day") \
                    .insertInto("default.matchs")
                
                print(f"✓ {valid_df.count()} enregistrements sauvegardés dans la table Hive 'matchs'")
            
                # Créer une table pour les statistiques de matchs par sport
                matchs_par_sport = valid_df.groupBy("sport_id", "sport_nom") \
                    .count() \
                    .withColumn("processed_time", current_timestamp())
                
                spark.sql("""
                    CREATE TABLE IF NOT EXISTS default.stats_matchs_par_sport (
                        sport_id INT,
                        sport_nom STRING,
                        nombre_matchs BIGINT,
                        processed_time TIMESTAMP
                    )
                    USING hive
                """)
                
                # Sauvegarder les statistiques
                matchs_par_sport.write \
                    .format("hive") \
                    .mode("append") \
                    .insertInto("default.stats_matchs_par_sport")
                
        except Exception as e:
            import traceback
            print(f"Erreur critique lors du traitement du batch matchs: {str(e)}")
            print(traceback.format_exc())
    
    # TRAITEMENT DES DONNÉES DES SPORTS
    # ---------------------------------
    def process_sports_batch(batch_df, batch_id):
        try:
            if batch_df.isEmpty():
                print(f"\n--- BATCH {batch_id} SPORTS VIDE ---")
                return
                
            print(f"\n--- DONNÉES BRUTES SPORTS (BATCH {batch_id}) ---")
            
            # Parser les données JSON
            parsed_df = batch_df.select(
                col("raw_data"),
                from_json(col("raw_data"), sport_schema).alias("data")
            )
            
            # Extraire les champs
            sports_df = parsed_df.select(
                col("data.sport_id").alias("sport_id"),
                col("data.nom").alias("nom"),
                col("data.categorie").alias("categorie"),
                col("data.nombre_joueurs").alias("nombre_joueurs"),
                col("data.description").alias("description"),
                current_timestamp().alias("processed_time")
            )
            
            # Filtrer pour ne garder que les sports valides
            valid_df = sports_df.filter(col("sport_id").isNotNull())
            
            if valid_df.count() > 0:
                print(f"--- {valid_df.count()} SPORTS VALIDES ---")
                
                # Créer la table Hive si elle n'existe pas
                spark.sql("""
                    CREATE TABLE IF NOT EXISTS default.sports (
                        sport_id INT,
                        nom STRING,
                        categorie STRING,
                        nombre_joueurs INT,
                        description STRING,
                        processed_time TIMESTAMP
                    )
                    USING hive
                """)
                
                # Sauvegarder dans Hive
                valid_df.write \
                    .format("hive") \
                    .mode("append") \
                    .insertInto("default.sports")
                
                print(f"✓ {valid_df.count()} enregistrements sauvegardés dans la table Hive 'sports'")
            
        except Exception as e:
            import traceback
            print(f"Erreur critique lors du traitement du batch sports: {str(e)}")
            print(traceback.format_exc())
    
    # TRAITEMENT DES DONNÉES DES UTILISATEURS
    # ---------------------------------------
    def process_users_batch(batch_df, batch_id):
        try:
            if batch_df.isEmpty():
                print(f"\n--- BATCH {batch_id} UTILISATEURS VIDE ---")
                return
                
            print(f"\n--- DONNÉES BRUTES UTILISATEURS (BATCH {batch_id}) ---")
            
            # Parser les données JSON
            parsed_df = batch_df.select(
                col("raw_data"),
                from_json(col("raw_data"), user_schema).alias("data")
            )
            
            # Extraire les champs
            users_df = parsed_df.select(
                col("data.user_id").alias("user_id"),
                col("data.prenom").alias("prenom"),
                col("data.nom").alias("nom"),
                col("data.email").alias("email"),
                col("data.date_inscription").cast(TimestampType()).alias("date_inscription"),
                col("data.sports_favoris").alias("sports_favoris"),
                col("data.notification_active").alias("notification_active"),
                current_timestamp().alias("processed_time")
            )
            
            # Filtrer pour ne garder que les utilisateurs valides
            valid_df = users_df.filter(col("user_id").isNotNull())
            
            if valid_df.count() > 0:
                print(f"--- {valid_df.count()} UTILISATEURS VALIDES ---")
                
                # Créer la table Hive si elle n'existe pas
                spark.sql("""
                    CREATE TABLE IF NOT EXISTS default.users (
                        user_id INT,
                        prenom STRING,
                        nom STRING,
                        email STRING,
                        date_inscription TIMESTAMP,
                        sports_favoris ARRAY<INT>,
                        notification_active BOOLEAN,
                        processed_time TIMESTAMP
                    )
                    USING hive
                """)
                
                # Sauvegarder dans Hive
                valid_df.write \
                    .format("hive") \
                    .mode("append") \
                    .insertInto("default.users")
                
                print(f"✓ {valid_df.count()} enregistrements sauvegardés dans la table Hive 'users'")
                
                # Calculer les statistiques des sports préférés
                
                # On va exploser l'array des sports favoris pour compter les occurrences
                exploded_df = valid_df.select(
                    col("user_id"),
                    expr("explode(sports_favoris)").alias("sport_id")
                )
                
                sport_counts = exploded_df.groupBy("sport_id").count().withColumnRenamed("count", "nombre_utilisateurs")
                
                spark.sql("""
                    CREATE TABLE IF NOT EXISTS default.stats_sports_favoris (
                        sport_id INT,
                        nombre_utilisateurs BIGINT,
                        processed_time TIMESTAMP
                    )
                    USING hive
                """)
                
                # Ajouter timestamp et sauvegarder les statistiques
                sport_counts_with_time = sport_counts.withColumn("processed_time", current_timestamp())
                
                sport_counts_with_time.write \
                    .format("hive") \
                    .mode("append") \
                    .insertInto("default.stats_sports_favoris")
                
                print(f"✓ Statistiques de sports favoris mises à jour")
            
        except Exception as e:
            import traceback
            print(f"Erreur critique lors du traitement du batch utilisateurs: {str(e)}")
            print(traceback.format_exc())
    
    # TRAITEMENT DES DONNÉES DES NOTIFICATIONS
    # ----------------------------------------
    def process_notifications_batch(batch_df, batch_id):
        try:
            if batch_df.isEmpty():
                print(f"\n--- BATCH {batch_id} NOTIFICATIONS VIDE ---")
                return
                
            print(f"\n--- DONNÉES BRUTES NOTIFICATIONS (BATCH {batch_id}) ---")
            
            # Parser les données JSON
            parsed_df = batch_df.select(
                col("raw_data"),
                from_json(col("raw_data"), notification_schema).alias("data")
            )
            
            # Extraire les champs
            notifications_df = parsed_df.select(
                col("data.notification_id").alias("notification_id"),
                col("data.user_id").alias("user_id"),
                col("data.match_id").alias("match_id"),
                col("data.type").alias("type"),
                col("data.contenu").alias("contenu"),
                col("data.date_envoi").cast(TimestampType()).alias("date_envoi"),
                col("data.statut").alias("statut"),
                current_timestamp().alias("processed_time")
            )
            
            # Filtrer pour ne garder que les notifications valides
            valid_df = notifications_df.filter(col("notification_id").isNotNull())
            
            if valid_df.count() > 0:
                print(f"--- {valid_df.count()} NOTIFICATIONS VALIDES ---")
                
                # Créer la table Hive si elle n'existe pas
                spark.sql("""
                    CREATE TABLE IF NOT EXISTS default.notifications (
                        notification_id INT,
                        user_id INT,
                        match_id INT,
                        type STRING,
                        contenu STRING,
                        date_envoi TIMESTAMP,
                        statut STRING,
                        processed_time TIMESTAMP
                    )
                    USING hive
                """)
                
                # Sauvegarder dans Hive
                valid_df.write \
                    .format("hive") \
                    .mode("append") \
                    .insertInto("default.notifications")
                
                print(f"✓ {valid_df.count()} enregistrements sauvegardés dans la table Hive 'notifications'")
                
                # Calculer les statistiques de notifications par type
                notif_by_type = valid_df.groupBy("type").count().withColumnRenamed("count", "nombre_notifications")
                
                spark.sql("""
                    CREATE TABLE IF NOT EXISTS default.stats_notifications_par_type (
                        type STRING,
                        nombre_notifications BIGINT,
                        processed_time TIMESTAMP
                    )
                    USING hive
                """)
                
                # Ajouter timestamp et sauvegarder les statistiques
                notif_stats_with_time = notif_by_type.withColumn("processed_time", current_timestamp())
                
                notif_stats_with_time.write \
                    .format("hive") \
                    .mode("append") \
                    .insertInto("default.stats_notifications_par_type")
                
                print(f"✓ Statistiques de notifications par type mises à jour")
            
        except Exception as e:
            import traceback
            print(f"Erreur critique lors du traitement du batch notifications: {str(e)}")
            print(traceback.format_exc())
    
    # Préparer les données pour le traitement par lot
    matchs_with_raw = json_df_matchs.withColumn("raw_data", col("value"))
    sports_with_raw = json_df_sports.withColumn("raw_data", col("value"))
    users_with_raw = json_df_users.withColumn("raw_data", col("value"))
    notifications_with_raw = json_df_notifications.withColumn("raw_data", col("value"))
    
    # DÉMARRAGE DES STREAMS
    # ---------------------
    
    # Stream pour traiter les données des sports
    sports_stream = sports_with_raw.writeStream \
        .foreachBatch(process_sports_batch) \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/checkpoints/sports") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    # Stream pour traiter les données des matchs
    matchs_stream = matchs_with_raw.writeStream \
        .foreachBatch(process_matchs_batch) \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/checkpoints/matchs") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    # Stream pour traiter les données des utilisateurs
    users_stream = users_with_raw.writeStream \
        .foreachBatch(process_users_batch) \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/checkpoints/users") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    # Stream pour traiter les données des notifications
    notifications_stream = notifications_with_raw.writeStream \
        .foreachBatch(process_notifications_batch) \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/checkpoints/notifications") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    print("=================================================")
    print("Traitement des données sportives démarré avec succès!")
    print("Traitement des données vers Hive en cours...")
    print("Stream 1: sports_topic -> table sports")
    print("Stream 2: matchs_topic -> table matchs")
    print("Stream 3: users_topic -> table users")
    print("Stream 4: notifications_topic -> table notifications")
    print("=================================================")
    print("Appuyez sur Ctrl+C pour arrêter...")
    
    try:
        # Attendre que les requêtes se terminent
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\nArrêt des streams...")
        sports_stream.stop()
        matchs_stream.stop()
        users_stream.stop()
        notifications_stream.stop()
        print("Streams arrêtés proprement.")
    finally:
        spark.stop()
        print("Session Spark fermée.")

if __name__ == "__main__":
    main() 