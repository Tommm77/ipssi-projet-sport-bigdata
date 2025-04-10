CREATE DATABASE IF NOT EXISTS kafka_data;
USE kafka_data;

CREATE EXTERNAL TABLE IF NOT EXISTS sports (
  sport_id INT,
  nom STRING,
  categorie STRING,
  nombre_joueurs INT,
  description STRING
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/kafka_data.db/sports';

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
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/kafka_data.db/matchs';

CREATE EXTERNAL TABLE IF NOT EXISTS users (
  user_id INT,
  prenom STRING,
  nom STRING,
  email STRING,
  date_inscription STRING,
  sports_favoris ARRAY<INT>,
  notification_active BOOLEAN
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/kafka_data.db/users';

CREATE EXTERNAL TABLE IF NOT EXISTS notifications (
  notification_id INT,
  user_id INT,
  match_id INT,
  type STRING,
  contenu STRING,
  date_envoi STRING,
  statut STRING
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/kafka_data.db/notifications';

SET hive.strict.checks.cartesian.product=false;

CREATE VIEW IF NOT EXISTS stats_matchs AS 
SELECT 
  sport_id, 
  sport_nom, 
  statut, 
  COUNT(*) as nombre_matchs, 
  AVG(score_domicile) as moyenne_score_domicile, 
  AVG(score_exterieur) as moyenne_score_exterieur 
FROM matchs 
GROUP BY sport_id, sport_nom, statut;

CREATE VIEW IF NOT EXISTS stats_sportifs AS 
SELECT 
  m.sport_id, 
  m.sport_nom, 
  COUNT(*) as nombre_matchs, 
  SUM(m.score_domicile) as total_buts_domicile, 
  SUM(m.score_exterieur) as total_buts_exterieur, 
  COUNT(DISTINCT m.equipe_domicile) + COUNT(DISTINCT m.equipe_exterieur) as nombre_equipes 
FROM matchs m 
GROUP BY m.sport_id, m.sport_nom;

CREATE VIEW IF NOT EXISTS stats_equipes_domicile AS 
SELECT 
  m.sport_id, 
  m.sport_nom, 
  m.equipe_domicile as equipe, 
  COUNT(*) as nombre_matchs_joues, 
  SUM(m.score_domicile) as buts_marques, 
  SUM(m.score_exterieur) as buts_encaisses 
FROM matchs m 
GROUP BY m.sport_id, m.sport_nom, m.equipe_domicile;

CREATE VIEW IF NOT EXISTS stats_equipes_exterieur AS 
SELECT 
  m.sport_id, 
  m.sport_nom, 
  m.equipe_exterieur as equipe, 
  COUNT(*) as nombre_matchs_joues, 
  SUM(m.score_exterieur) as buts_marques, 
  SUM(m.score_domicile) as buts_encaisses 
FROM matchs m 
GROUP BY m.sport_id, m.sport_nom, m.equipe_exterieur; 

CREATE VIEW IF NOT EXISTS stats_notifications AS 
SELECT 
  n.type, 
  COUNT(*) as nombre_notifications, 
  COUNT(DISTINCT n.user_id) as nombre_utilisateurs_distincts, 
  COUNT(DISTINCT n.match_id) as nombre_matchs_distincts 
FROM notifications n 
GROUP BY n.type;

CREATE VIEW IF NOT EXISTS stats_equipes_completes AS
SELECT 
  d.sport_id,
  d.sport_nom,
  d.equipe,
  (d.nombre_matchs_joues + COALESCE(e.nombre_matchs_joues, 0)) as total_matchs,
  (d.buts_marques + COALESCE(e.buts_marques, 0)) as total_buts_marques,
  (d.buts_encaisses + COALESCE(e.buts_encaisses, 0)) as total_buts_encaisses
FROM stats_equipes_domicile d
LEFT JOIN stats_equipes_exterieur e ON d.sport_id = e.sport_id AND d.equipe = e.equipe
UNION
SELECT 
  e.sport_id,
  e.sport_nom,
  e.equipe,
  (e.nombre_matchs_joues + COALESCE(d.nombre_matchs_joues, 0)) as total_matchs,
  (e.buts_marques + COALESCE(d.buts_marques, 0)) as total_buts_marques,
  (e.buts_encaisses + COALESCE(d.buts_encaisses, 0)) as total_buts_encaisses
FROM stats_equipes_exterieur e
LEFT JOIN stats_equipes_domicile d ON e.sport_id = d.sport_id AND e.equipe = d.equipe
WHERE d.equipe IS NULL; 