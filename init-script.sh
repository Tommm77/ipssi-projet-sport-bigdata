#!/bin/bash
set -e

# Fonction pour vérifier si un port est ouvert
check_port() {
    local host=$1
    local port=$2
    local timeout=$3
    
    timeout $timeout bash -c "cat < /dev/null > /dev/tcp/$host/$port"
    return $?
}

# Définir explicitement l'adresse du namenode
export HADOOP_NAMENODE="namenode"
echo "Configuring core"
echo " - Setting fs.defaultFS=hdfs://${HADOOP_NAMENODE}:8020"
echo "Configuring hdfs"
echo "Configuring yarn"
echo "Configuring httpfs"
echo "Configuring kms"
echo "Configuring mapred"
echo "Configuring hive"
echo "Configuring for multihomed network"

# Attendre que les services soient disponibles
echo "Attente du démarrage de HDFS..."
MAX_RETRIES=30
RETRY_COUNT=0

while ! hdfs dfs -ls hdfs://${HADOOP_NAMENODE}:8020/ > /dev/null 2>&1; do
  echo "En attente de HDFS... (tentative $((RETRY_COUNT+1))/$MAX_RETRIES)"
  RETRY_COUNT=$((RETRY_COUNT+1))
  
  if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
    echo "ERREUR: HDFS n'est pas disponible après $MAX_RETRIES tentatives"
    exit 1
  fi
  
  sleep 5
done

echo "HDFS est disponible!"

echo "Attente du démarrage du Hive Metastore..."
RETRY_COUNT=0
MAX_METASTORE_RETRIES=60  # Augmenter le nombre de tentatives pour le metastore
METASTORE_SLEEP=10  # Augmenter le délai entre les tentatives

while ! check_port hive-metastore 9083 1; do
  echo "En attente du Hive Metastore... (tentative $((RETRY_COUNT+1))/$MAX_METASTORE_RETRIES)"
  RETRY_COUNT=$((RETRY_COUNT+1))
  
  if [ $RETRY_COUNT -ge $MAX_METASTORE_RETRIES ]; then
    echo "ERREUR: Hive Metastore n'est pas disponible après $MAX_METASTORE_RETRIES tentatives"
    exit 1
  fi
  
  sleep $METASTORE_SLEEP
done

echo "Hive Metastore est disponible!"

echo "Attente du démarrage du Hive Server..."
RETRY_COUNT=0
MAX_HIVESERVER_RETRIES=60  # Augmenter le nombre de tentatives pour le hive server

while ! check_port hive-server 10000 1; do
  echo "En attente du Hive Server... (tentative $((RETRY_COUNT+1))/$MAX_HIVESERVER_RETRIES)"
  RETRY_COUNT=$((RETRY_COUNT+1))
  
  if [ $RETRY_COUNT -ge $MAX_HIVESERVER_RETRIES ]; then
    echo "ERREUR: Hive Server n'est pas disponible après $MAX_HIVESERVER_RETRIES tentatives"
    exit 1
  fi
  
  sleep 10
done

echo "Hive Server est disponible!"

# Créer les répertoires nécessaires dans HDFS
echo "Configuration des répertoires HDFS..."
hdfs dfs -mkdir -p /tmp || echo "Le répertoire /tmp existe déjà"
hdfs dfs -mkdir -p /user/hive/warehouse || echo "Le répertoire /user/hive/warehouse existe déjà"
hdfs dfs -mkdir -p /user/hive/warehouse/kafka_data.db/sports || echo "Le répertoire sports existe déjà"
hdfs dfs -mkdir -p /user/hive/warehouse/kafka_data.db/matchs || echo "Le répertoire matchs existe déjà"
hdfs dfs -mkdir -p /user/hive/warehouse/kafka_data.db/users || echo "Le répertoire users existe déjà"
hdfs dfs -mkdir -p /user/hive/warehouse/kafka_data.db/notifications || echo "Le répertoire notifications existe déjà"

# Définir les permissions appropriées
echo "Configuration des permissions HDFS..."
hdfs dfs -chmod -R 777 /tmp
hdfs dfs -chmod -R 777 /user/hive/warehouse

# Exécuter le script SQL pour créer les tables Hive
echo "Initialisation des tables Hive..."
beeline -u jdbc:hive2://hive-server:10000 -f /opt/init-hive.sql

if [ $? -eq 0 ]; then
  echo "Initialisation terminée avec succès!"
  
  # Vérifier que les tables ont été créées
  echo "Vérification des tables créées:"
  beeline -u jdbc:hive2://hive-server:10000 -e "USE kafka_data; SHOW TABLES;"
else
  echo "ERREUR: L'initialisation des tables Hive a échoué!"
  exit 1
fi 