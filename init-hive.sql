-- Création de la base de données
CREATE DATABASE IF NOT EXISTS kafka_data;
USE kafka_data;

-- Table pour les sports
CREATE EXTERNAL TABLE IF NOT EXISTS sports (
  sport_id INT,
  nom STRING,
  categorie STRING,
  nombre_joueurs INT,
  description STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/kafka_data.db/sports';

-- Table pour les matchs
CREATE EXTERNAL TABLE IF NOT EXISTS matchs (
  match_id INT,
  sport_id INT,
  sport_nom STRING,
  equipe_domicile STRING,
  equipe_exterieur STRING,
  score_domicile INT,
  score_exterieur INT,
  date_match STRING,
  lieu STRING,
  statut STRING,
  derniere_mise_a_jour STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/kafka_data.db/matchs';

-- Table pour les utilisateurs
CREATE EXTERNAL TABLE IF NOT EXISTS users (
  user_id INT,
  prenom STRING,
  nom STRING,
  email STRING,
  date_inscription STRING,
  sports_favoris ARRAY<INT>,
  notification_active BOOLEAN
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/kafka_data.db/users';

-- Table pour les notifications
CREATE EXTERNAL TABLE IF NOT EXISTS notifications (
  notification_id INT,
  user_id INT,
  match_id INT,
  type STRING,
  contenu STRING,
  date_envoi STRING,
  statut STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/kafka_data.db/notifications'; 