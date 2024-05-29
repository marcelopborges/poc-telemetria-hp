import hashlib
import json
import logging
from datetime import datetime, timedelta
import requests
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from requests import get, post

last_call_timestamp = None
url_api_sianet = Variable.get("url_api_sianet")
auth_token_basic_sianet = Variable.get("auth_token_basic_sianet")
url_trip_made_sianet = Variable.get("url_trip_made_sianet")


token_data = {
    "token": None,
    "timestamp": None
}


def generate_hash(*args):
    """
    Gera um hash SHA-256 a partir dos valores fornecidos.
    """
    hash_input = "".join(args)
    return hashlib.sha256(hash_input.encode('utf-8')).hexdigest()


def convert_datetime_with_time(date_str):
    """
    Converte uma string de data e hora do formato 'DD/MM/YYYY HH:MM' para o formato 'YYYY-MM-DD HH:MM'.
    Retorna None se a entrada for None ou se a conversão falhar.
    """
    if date_str is None:
        return None
    try:
        return datetime.strptime(date_str, '%d/%m/%Y %H:%M').strftime('%Y-%m-%d %H:%M')
    except ValueError:
        logging.error(f"Data e hora fornecida inválida: {date_str}")
        return None

def convert_datetime_without_time(date_str):
    """
    Converte uma string de data do formato 'DD/MM/YYYY' para o formato 'YYYY-MM-DD'.
    Retorna None se a entrada for None ou se a conversão falhar.
    """
    if date_str is None:
        return None
    try:
        return datetime.strptime(date_str, '%d/%m/%Y').strftime('%Y-%m-%d')
    except ValueError:
        logging.error(f"Data fornecida inválida: {date_str}")
        return None



def get_token_sianet():
    """

    Função para receber o token bearer do sianet. Respeitando o tempo de tiva de 30 minutos

    """
    global token_data
    if token_data["token"] is not None and (datetime.now() - token_data["timestamp"]) < timedelta(seconds=1800):
        return token_data["token"]
    else:
        url = f"{url_api_sianet}"
        payload = {}
        headers = {
            'Authorization': f'Basic {auth_token_basic_sianet}'
        }
        response = post(url, headers=headers, data=payload)
        if response.status_code == 200:
            data = response.json()
            return data['token']
        else:
            raise Exception(f"Failed to get token: HTTP {response.status_code}")


def get_data_trip_made_sianet(**kwargs):
    logical_date = kwargs['logical_date']
    logical_date = logical_date - timedelta(days=1)  # D-10
    formatted_date = logical_date.strftime("%d/%m/%Y")

    url = f"http://siannet.gestaosian.com/api/ConsultaViagens?data={formatted_date}&viagens=programadas"
    headers = {"Authorization": f"Bearer {get_token_sianet()}"}  # Certifique-se de que get_token_sianet() está definida

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = json.loads(response.text)
        # Extração dos dados relevantes a partir da estrutura da resposta
        programadas_data = []
        for key, value in data.get('dados', {}).get('programadas', {}).items():
            programadas_data.extend(value)
        insert_data_into_postgres(programadas_data)
    else:
        raise Exception(f"Failed to get data: HTTP {response.status_code}")


def insert_data_into_postgres(data):
    hook = PostgresHook(postgres_conn_id='postgres_con_id')
    conn = hook.get_conn()
    cursor = conn.cursor()

    insert_query = """
    INSERT INTO hp.raw_escalas_programadas (data, linha, carro, re, nome, dthr_saida, dthr_retorno, dthr_chegada)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """

    rows_to_insert = []
    for entry in data:
        try:
            row = (
                convert_datetime_without_time(entry.get('DATA')),
                entry.get('LINHA'),
                entry.get('CARRO'),
                entry.get('RE'),
                entry.get('NOME'),
                convert_datetime_with_time(entry.get('DTHR_SAIDA')) if entry.get('DTHR_SAIDA') else None,
                convert_datetime_with_time(entry.get('DTHR_RETORNO')) if entry.get('DTHR_RETORNO') else None,
                convert_datetime_with_time(entry.get('DTHR_CHEGADA')) if entry.get('DTHR_CHEGADA') else None
            )
            rows_to_insert.append(row)
        except Exception as e:
            logging.error(f"Error processing row {entry}: {e}")

    cursor.executemany(insert_query, rows_to_insert)
    conn.commit()
    cursor.close()
    conn.close()



def insert_dag_metadata_schedule(**kwargs):
    ti = kwargs['ti']
    start_time = ti.xcom_pull(key='start_time', task_ids='mark_start')
    end_time = ti.xcom_pull(key='end_time', task_ids='mark_end')

    if not start_time or not end_time:
        error_msg = f"Start time or end time not set correctly. Start: {start_time}, End: {end_time}"
        print(error_msg)
        raise ValueError(error_msg)

    duration = (end_time - start_time).total_seconds()

    pg_hook = PostgresHook(postgres_conn_id='postgres_con_id')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    dag_id = kwargs['dag_run'].dag_id
    execution_date = kwargs['ds']

    success = True
    error_message = None

    insert_query = """
    INSERT INTO hp.pipeline_metadata (dag_id, execution_date, start_date, end_date, duration, success, error_message)
    VALUES (%s, %s, %s, %s, %s, %s, %s);
    """
    cursor.execute(insert_query, (dag_id, execution_date, start_time, end_time, duration, success, error_message))
    conn.commit()
    cursor.close()
    conn.close()


#
def mark_start(**context):
    start = datetime.now()
    context['ti'].xcom_push(key='start_time', value=start)
    print(f"Mark start at {start}")


def mark_end(**context):
    end = datetime.now()
    context['ti'].xcom_push(key='end_time', value=end)
    print(f"Mark end at {end}")


# """
#
# Esta DAG foi desenvolvida para ser executada de forma manual, devido o seu tamanho e a baixa incidencia de mudanças.
#
# """


@dag(start_date=datetime(2024, 2, 26), schedule='30 11 * * *', catchup=True,
     tags=['airbyte', 'HP', 'Sianet'])
def pipeline_hp_sianet_scheduler():
    start = EmptyOperator(task_id='start')
    start_task = PythonOperator(
        task_id='mark_start',
        python_callable=mark_start,
        provide_context=True,
        retries=5,
        retry_delay=timedelta(minutes=5)
    )
    get_token = PythonOperator(
        task_id='get_token',
        python_callable=get_token_sianet,
        provide_context=True,
        retries=5,
        retry_delay=timedelta(minutes=5)
    )
    get_data_line = PythonOperator(
        task_id='get_data_line',
        python_callable=get_data_trip_made_sianet,
        provide_context=True,
        retries=5,

        retry_delay=timedelta(minutes=5)

    )
    end_task = PythonOperator(
        task_id='mark_end',
        python_callable=mark_end,
        provide_context=True,
        retries=5,

        retry_delay=timedelta(minutes=5)
    )
    create_metadata_schedule = PythonOperator(
        task_id='create_metadata_data_schedule',
        python_callable=insert_dag_metadata_schedule,
        provide_context=True,
        retries=5,
        retry_delay=timedelta(minutes=10)
    )
    end = EmptyOperator(task_id='end')

    start >> start_task >> get_token >> get_data_line >> end_task >> create_metadata_schedule >> end


dag = pipeline_hp_sianet_scheduler()