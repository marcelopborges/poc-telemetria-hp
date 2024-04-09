import time
import pytz
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from requests import get, post
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
import hashlib

last_call_timestamp = None
url_identity = Variable.get("url_identity")
username_access_identity_mix = Variable.get("username_access_identity_mix")
password_access_identity_mix = Variable.get("password_access_identity_mix")
auth_token_basic_mix_identity = Variable.get("auth_token_basic_mix_identity")
id_hp_mix = Variable.get("id_hp_mix")
token_data = {
    "token": None,
    "timestamp": None
}


def mark_start(**context):
    context['ti'].xcom_push(key='start_time', value=datetime.now())


def get_token_bearer():
    """

    Função para obter token bearer

    """
    global token_data
    if token_data["token"] is not None and (datetime.now() - token_data["timestamp"]) < timedelta(seconds=3600):
        return token_data["token"]
    else:
        url = f"{url_identity}"
        payload = {
            'grant_type': 'password',
            'username': f'{username_access_identity_mix}',
            'password': f'{password_access_identity_mix}',
            'scope': 'offline_access MiX.Integrate'
        }
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': f'Basic {auth_token_basic_mix_identity}'
        }
        response = post(url, headers=headers, data=payload)
        if response.status_code == 200:
            data = response.json()
            return data['access_token']
        else:
            raise Exception(f"Failed to get token: HTTP {response.status_code}")


def generate_hash(*args):
    """
    Gera um hash SHA-256 a partir dos valores fornecidos.
    """
    hash_input = "".join(args)
    return hashlib.sha256(hash_input.encode('utf-8')).hexdigest()


def get_geodata(**kwargs):
    """
    Função para obter dados do endpoint - geodata
    """

    global last_call_timestamp
    min_interval = 60 / 7  #limite imposto de 7 requisições por minuto segundo documentação
    if last_call_timestamp is not None:
        elapsed_time = time.time() - last_call_timestamp
        if elapsed_time < min_interval:
            time_to_wait = min_interval - elapsed_time
            time.sleep(time_to_wait)
    last_call_timestamp = time.time()
    execution_date = kwargs['execution_date']
    execution_date = execution_date - timedelta(days=1)  # requisição D-1
    formatted_date = execution_date.strftime("%Y%m%d")
    url_stream = f"https://integrate.us.mixtelematics.com/api/geodata/assetmovements/{id_hp_mix}/{formatted_date}000000/{formatted_date}235959"
    headers = {"Authorization": f"Bearer {get_token_bearer()}"}
    response = get(url_stream, headers=headers)
    if response.status_code == 200:
        return response.text
    else:
        raise Exception(f"Failed to get geodata: HTTP {response.status_code}")


def process_and_load_geodata_to_postgres(**kwargs):
    """
    Função para enriquecer os dados obtidos no endpoint - geodata, principalmente na inserção de data que é inexistente,
    e precisa ser em tempo de execução.

    """
    execution_date = kwargs['execution_date']
    geodata_json = kwargs['ti'].xcom_pull(task_ids='get_geodata')
    data = json.loads(geodata_json)
    execution_date = execution_date - timedelta(days=1)
    formatted_date = execution_date.strftime("%Y-%m-%d")
    features = data.get("features", [])

    pg_hook = PostgresHook(postgres_conn_id='postgres_con_id')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    insert_query = """
    INSERT INTO hp.geodata (registration, description, geometry_type, coordinates, execution_date, hash)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (hash) DO NOTHING
    """

    for feature in features:
        properties = feature.get("properties", {})
        geometry = feature.get("geometry", {})

        registration = properties.get("Registration", "")
        description = properties.get("Description", "")
        geometry_type = geometry.get("type", "")
        coordinates = json.dumps(geometry.get("coordinates", []))
        unique_hash = generate_hash(registration, description, formatted_date)

        try:
            cursor.execute(insert_query,
                           (registration, description, geometry_type, coordinates, formatted_date, unique_hash))
        except Exception as e:
            connection.rollback()
            raise
        else:
            connection.commit()

    cursor.close()
    connection.close()


def insert_data_into_postgres(data):
    """

    Função para inserir dados no banco de dados postgres.

    """
    pg_hook = PostgresHook(postgres_conn_id='postgres_con_id')
    insert_query = """
    INSERT INTO hp.geodata (registration, description, geometry_type, coordinates, execution_date)
    VALUES (%s, %s, %s, %s, %s)
    """
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    try:
        cursor.executemany(insert_query, data)
        connection.commit()
    except Exception as e:
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()


def insert_dag_metadata(**kwargs):
    """

    Função para criar alguns metadados em tempo de execução.

    """
    start_time = kwargs['ti'].xcom_pull(key='start_time')
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    pg_hook = PostgresHook(postgres_conn_id='postgres_con_id')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    dag_id = kwargs['dag_run'].dag_id
    execution_date = kwargs['ds']
    start_date = kwargs.get('start_date', datetime.now(pytz.utc))
    end_date = kwargs.get('end_date', datetime.now(pytz.utc))

    if start_date.tzinfo is None or start_date.tzinfo.utcoffset(start_date) is None:
        start_date = pytz.utc.localize(start_date)
    if end_date.tzinfo is None or end_date.tzinfo.utcoffset(end_date) is None:
        end_date = pytz.utc.localize(end_date)

    duration = (end_date - start_date).total_seconds()

    success = True
    error_message = None

    insert_query = """
    INSERT INTO hp.pipeline_metadata (dag_id, execution_date, start_date, end_date, duration, success, error_message)
    VALUES (%s, %s, %s, %s, %s, %s, %s);
    """

    cursor.execute(insert_query, (dag_id, execution_date, start_date, end_date, duration, success, error_message))
    conn.commit()
    cursor.close()
    conn.close()


"""

Esta dag foi desenvolvida para ajudar a avaliar as possibilidades da utilização de telemetria da Mix Telemetrics.
Está programada para atualizar de segunda a sexta-feira as 12:30 (UTC) 09:30 horário de Brasília.
Está configurada para realizar 3 tentativas em caso de algum erro sendo atribuidos 5 minutos a cada tentativa, somente
 após 3 falhas será considerado carga com error.
 
"""


@dag(start_date=datetime(2024, 2, 26), schedule='30 12 * * MON-FRI', catchup=True,
     tags=['airbyte', 'HP', 'Mix-Telematics'])
def pipeline_hp_telemetics():
    """
    DAG personalizada para extrair dados da API Mix-Telematics
    """

    start = EmptyOperator(task_id='start')

    start_task = PythonOperator(
        task_id='mark_start',
        python_callable=mark_start,
        provide_context=True,
    )

    get_bearer_token = PythonOperator(
        task_id='get_bearer_token',
        python_callable=get_token_bearer
    )

    get_data = PythonOperator(
        task_id='get_geodata',
        python_callable=get_geodata,
        provide_context=True,
        retries=3,
        retry_delay=timedelta(minutes=1)
    )

    process_data = PythonOperator(
        task_id='process_data',
        python_callable=process_and_load_geodata_to_postgres,
        provide_context=True
    )

    create_metadata = PythonOperator(
        task_id='create_metadata',
        python_callable=insert_dag_metadata,
        provide_context=True
    )

    end = EmptyOperator(task_id='end')

    start >> start_task >> get_bearer_token >> get_data >> process_data >> create_metadata >> end


dag = pipeline_hp_telemetics()
