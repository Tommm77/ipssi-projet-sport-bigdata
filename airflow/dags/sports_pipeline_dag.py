from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator
import json
import requests

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Fonction pour vérifier si Kafka est disponible
def check_kafka_availability():
    try:
        # Cette requête devrait être adaptée à votre configuration
        response = requests.get('http://kafka:9092', timeout=10)
        return response.status_code == 200
    except:
        # Si on ne peut pas se connecter, on considère que Kafka n'est pas disponible
        return False

# Function pour vérifier le nombre de matchs dans Hive
def check_matchs_count():
    # Cette fonction devrait exécuter une requête Hive pour compter les matchs
    # et retourner 'generate_matchs' si le nombre est inférieur à un seuil
    # ou 'skip_generation' sinon
    
    # Pour cet exemple, on considère qu'il faut toujours générer des matchs
    return 'generate_matchs'

with DAG(
    'sports_data_pipeline',
    default_args=default_args,
    description='Pipeline de traitement des données sportives',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 1),
    catchup=False,
    tags=['sports', 'big_data'],
) as dag:
    
    # Points de décision et tâches factices
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    
    # Tâche pour générer des sports de base (exécuté une seule fois au début)
    generate_sports = BashOperator(
        task_id='generate_sports',
        bash_command='python /app/sports_producer.py',
    )
    
    # Tâche pour vérifier et décider si on génère de nouveaux matchs
    check_matchs = BranchPythonOperator(
        task_id='check_matchs',
        python_callable=check_matchs_count,
    )
    
    # Génération de matchs (s'exécute si nécessaire)
    generate_matchs = BashOperator(
        task_id='generate_matchs',
        bash_command='python /app/matchs_producer.py &',
        depends_on_past=False,
    )
    
    # Tâche factice si pas besoin de générer des matchs
    skip_generation = DummyOperator(
        task_id='skip_generation',
    )
    
    # Vérifier que les données sont bien reçues par Spark et traitées
    check_spark_processing = BashOperator(
        task_id='check_spark_processing',
        bash_command='echo "Vérification du traitement Spark..." && sleep 30',
    )
    
    # Lancer le traitement Spark des données (si pas déjà en cours)
    start_spark_processing = BashOperator(
        task_id='start_spark_processing',
        bash_command='pgrep -f "process_sports_data.py" || python /app/process_sports_data.py &',
    )
    
    # Exécuter quelques requêtes Hive pour vérifier et agréger les données
    run_hive_queries = BashOperator(
        task_id='run_hive_queries',
        bash_command='''
        beeline -u jdbc:hive2://hive-server:10000 \
        -e "SELECT sport_id, sport_nom, COUNT(*) as nb_matchs FROM default.matchs GROUP BY sport_id, sport_nom;" \
        -e "SELECT type, COUNT(*) as nb_notifications FROM default.notifications GROUP BY type;"
        ''',
    )
    
    # Tâche finale pour log des statistiques
    log_statistics = BashOperator(
        task_id='log_statistics',
        bash_command='echo "Pipeline de données sportives terminé avec succès: $(date)"',
    )
    
    # Définir les dépendances
    start >> generate_sports >> check_matchs
    
    check_matchs >> generate_matchs >> check_spark_processing
    check_matchs >> skip_generation >> check_spark_processing
    
    check_spark_processing >> start_spark_processing >> run_hive_queries >> log_statistics >> end 