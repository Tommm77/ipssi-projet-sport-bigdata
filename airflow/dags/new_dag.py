from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
# OU utilisez Impyla comme alternative
from datetime import datetime
from pyhive import hive

def fn():
    # Connect to Hive
    conn = hive.Connection(host='hive-server', port='10000', database='default', auth='NONE')
    cursor = conn.cursor()

    # Execute query
    cursor.execute('SELECT * FROM produits2 where prix >=2000')
    result = cursor.fetchone()
    print(result)
    cursor.close()
    conn.close()
    return 'action1' if result else 'action2'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    'hive_branch',
    default_args=default_args,
    schedule_interval=None
) as dag:

    hive_op = BranchPythonOperator(
        task_id='hive_query',
        python_callable=fn,
    )
    action1 = PythonOperator(
        task_id='action1',
        python_callable=lambda: print("Action 1 executed"),

    )
    action2 = PythonOperator(
        task_id='action2',
        python_callable=lambda: print("Action 2 executed"),

    )
    end = PythonOperator(
        task_id='end',
        python_callable=lambda: print("Action 3 executed"),
        trigger_rule='none_failed',

    )
    hive_op >> [action1, action2] >> end

