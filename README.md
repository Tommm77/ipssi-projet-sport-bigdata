# Application de Gestion de Données Sportives

## 1. Description du Projet

### Vue d'ensemble
Ce projet est une application Big Data complète pour la gestion, le traitement et la visualisation de données sportives en temps réel. L'architecture est basée sur diverses technologies Big Data modernes connectées entre elles pour former un pipeline de données robuste, depuis l'ingestion jusqu'à la visualisation.

### Architecture du Système

L'application est divisée en plusieurs composants interconnectés:

#### 1. Sources de Données et Ingestion
- **Producteurs Kafka** : Génèrent et envoient des données vers Kafka concernant:
  - Sports (types de sports, règles, etc.)
  - Matchs (résultats, horaires, équipes)
  - Utilisateurs (profils, préférences)
  - Notifications (alertes de matchs, résultats)
- **Apache Kafka** : Sert de couche de messaging pour toutes les données en temps réel
- **Apache NiFi** : Effectue des appels API périodiques vers des services externes et transmet les données récupérées à Kafka
- **Apache Airflow** : Génère des notifications périodiques pour les événements sportifs et les envoie vers Kafka

#### 2. Stockage et Traitement
- **HDFS** : Stockage distribué pour les données persistantes
- **Apache Hive** : Entrepôt de données pour le stockage structuré et les requêtes SQL
- **Apache Spark** : Traitement des flux de données en temps réel et analyses complexes
- **Apache Airflow** : Orchestration des tâches et workflows

#### 3. Interface Utilisateur
- **Frontend React** : Interface web pour les utilisateurs finaux
- **Backend FastAPI** : API REST qui consomme les données de Kafka et les expose aux clients
- **Apache Superset** : Tableau de bord pour la visualisation et l'analyse des données

### Flux de Données
1. Les producteurs génèrent des données (sports, matchs, utilisateurs)
2. NiFi effectue des appels API périodiques et transmet les données à Kafka
3. Airflow génère des notifications et les publie sur le topic Kafka dédié
4. Ces données sont publiées sur des topics Kafka
5. Spark consomme les flux Kafka et effectue des transformations
6. Les données transformées sont stockées dans Hive sur HDFS
7. Le backend consomme directement les données de Kafka et les expose via une API REST
8. Le frontend affiche les données aux utilisateurs finaux
9. Airflow orchestre des tâches périodiques pour le pipeline de données
10. Superset interroge Hive pour l'analyse avancée et la visualisation des données

### Fonctionnalités Clés
- Suivi en temps réel des matchs sportifs
- Système de notification pour les événements sportifs
- Analyses statistiques des performances des équipes
- Interface utilisateur intuitive pour naviguer dans les données
- Tableaux de bord interactifs pour l'analyse des données
- Pipeline de données automatisé et orchestré

## 2. Guide d'Installation

### Prérequis
- Docker et Docker Compose
- Git
- Au moins 12 Go de RAM disponible
- Connexion Internet (pour télécharger les images Docker)

### Étapes d'Installation

#### 1. Cloner le Répertoire
```bash
git clone <URL_DU_REPO>
cd <NOM_DU_REPO>
```

#### 2. Configuration des Variables d'Environnement
```bash
# Création ou modification du fichier .env (facultatif - déjà configuré par défaut)
echo "FOOTBALL_API_KEY=votre_clé_api" > .env
```

#### 3. Lancement des Services
```bash
# Démarrer l'ensemble des services
docker-compose up -d
```

Ce script effectue les actions suivantes:
1. Configure Hadoop pour utiliser le namenode correct
2. Attend que les services HDFS, Hive Metastore et Hive Server soient disponibles
3. Crée les répertoires nécessaires dans HDFS
4. Configure les permissions appropriées
5. Initialise les tables Hive avec init-hive.sql

#### 5. Vérification de l'Installation
Après l'initialisation, les services suivants devraient être disponibles:

- **Frontend**: http://localhost:3000
- **Backend API**: http://localhost:8000
- **Hadoop NameNode**: http://localhost:50070
- **Airflow**: http://localhost:18080 (utilisateur: admin, mot de passe: admin)
- **Superset**: http://localhost:8089 (utilisateur: admin, mot de passe: admin)
- **NiFi**: http://localhost:8082

### Description des processus en cours

Une fois le système démarré, les processus suivants sont en action:

1. **Ingestion de données**:
   - NiFi effectue des appels API périodiques et transmet les données à Kafka
   - Les producteurs Kafka génèrent et envoient des données vers leurs topics respectifs
   - Airflow génère des notifications de matchs de football et les envoie vers Kafka
   - Le service kafka-to-hive transfère les données des topics Kafka vers Hive

2. **Traitement**:
   - Spark traite les données en streaming depuis Kafka
   - Hive stocke les données structurées et permet les requêtes
   - Airflow exécute périodiquement des DAGs, notamment pour générer des notifications

3. **Interfaces**:
   - Le backend FastAPI consomme directement les données de Kafka et les expose via des points d'API REST
   - Le frontend React interroge ces API et affiche les informations
   - Superset permet la création de visualisations et tableaux de bord à partir des données Hive

### Dépannage

1. **En cas de problème avec Kafka**:
   ```bash
   docker-compose restart kafka zookeeper
   ```

2. **Si les données n'apparaissent pas dans Hive**:
   ```bash
   docker-compose restart hive-server hive-metastore
   ```

3. **Pour redémarrer complètement l'application**:
   ```bash
   docker-compose down
   docker-compose up -d
   ./init-script.sh
   ```

4. **Pour consulter les logs d'un service spécifique**:
   ```bash
   docker-compose logs -f <nom_du_service>
   ```
   
5. **Pour rafraîchir Airflow**:
   ```bash
   ./refresh_airflow.sh
   ```

6. **Si NiFi ne fonctionne pas correctement**:
   ```bash
   docker-compose restart nifi
   ``` 
