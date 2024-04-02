from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow.models import Variable
import requests

api_url = Variable.get("airbyte_api_url")
geodata_id = Variable.get("geodata_airbyte_id")

def update_date_connection():
    response = requests.get(f'{api_url}/connections/{geodata_id}')
    if response.status_code != 200:
        raise Exception("Falha ao obter a configuração da conexão do Airbyte.")
    current_config = response.json()


@dag(start_date=datetime(2024, 3, 25), schedule=None, catchup=False, tags=['airbyte'])
def pipeline_telemetrics():
    """

    """
    start = EmptyOperator(
        task_id='start',
    )

    load_geodata_raw = AirbyteTriggerSyncOperator(
        task_id='load_geodata_raw',
        airbyte_conn_id='airbyte',
        connection_id=Variable.get('geodata_airbyte_id')
        # connection_id=AIRBYTE_JOB_ID_LOAD_CUSTOMER_TRANSACTIONS_RAW
    )
    start >> load_geodata_raw


dag = pipeline_telemetrics()

