import hashlib
import json
import logging
from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from requests import get, post

last_call_timestamp = None
url_api_sianet = Variable.get("url_api_sianet")
auth_token_basic_sianet = Variable.get("auth_token_basic_sianet")
url_api_sianet_trajeto_ocioso_emp2 = Variable.get("url_api_sianet_trajeto_ocioso_emp2")

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


def get_token_sianet():
    """

    Função para receber o token bearer do sianet. Respeitando o tempo de tiva de 30 minutos

    """
    global token_data
    if token_data["token"] is not None and (datetime.now() - token_data["timestamp"]) < timedelta(seconds=1800):
        return token_data["token"]
    else:
        url = f"{auth_token_basic_sianet}"
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


def get_data_line_sianet():
    """

    Função que faz a requisição dos dados na API do sianet

    """
    url = f"{url_api_sianet_trajeto_ocioso_emp2}"
    headers = {"Authorization": f"Bearer {get_token_sianet()}"}
    response = get(url, headers=headers)
    if response.status_code == 200:
        return response.text
    else:
        raise Exception(f"Failed to get geodata: HTTP {response.status_code}")


def insert_line_data(**kwargs):
    """

    Função responsável para inserir os dados no banco de dados.

    """
    ti = kwargs['ti']
    linha_data_json = ti.xcom_pull(task_ids='get_data_line')
    linha_data = json.loads(linha_data_json)
    pg_hook = PostgresHook(postgres_conn_id='postgres_con_id')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    try:
        for k, v in linha_data['dados'].items():
            linha = v['linha']['numLinha']
            descricao = v['linha']['descricao']
            insert_linha_sql = """
            INSERT INTO hp.linhas (num_linha, descricao)
            VALUES (%s, %s)
            ON CONFLICT (num_linha) DO NOTHING
            RETURNING id;
            """
            cursor.execute(insert_linha_sql, (linha, descricao))
            linha_id = cursor.fetchone()

            if linha_id:
                linha_id = linha_id[0]
            else:
                cursor.execute("SELECT id FROM hp.linhas WHERE num_linha = %s;", (linha,))
                linha_id = cursor.fetchone()[0]
            for direcao in ['ida', 'volta']:
                pontos_trajeto = v['trajeto'].get(direcao, {}).get('PONTOS_TRAJETO', [])
                if pontos_trajeto:  # Verifica se a lista de pontos não está vazia
                    pontos_geolocalizacao_json = json.dumps(pontos_trajeto)
                    insert_trajeto_sql = """
                    INSERT INTO hp.trajetos (linha_id, direcao, pontos_geolocalizacao)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (linha_id, direcao) DO UPDATE SET pontos_geolocalizacao = EXCLUDED.pontos_geolocalizacao;
                    """
                    cursor.execute(insert_trajeto_sql, (linha_id, direcao, pontos_geolocalizacao_json))
            connection.commit()
    except Exception as e:
        logging.error(f"Ocorreu um erro: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()


def mark_start(**context):
    context['ti'].xcom_push(key='start_time', value=datetime.now())


"""

Esta DAG foi desenvolvida para ser executada de forma manual, devido o seu tamanho e a baixa incidencia de mudanças.

"""


@dag(start_date=datetime(2024, 4, 5), schedule=None, catchup=False,
     tags=['airbyte', 'HP', 'Sianet'])
def pipeline_hp_sianet():
    """

    DAG para operações do Sianet

    """

    start = EmptyOperator(task_id='start')
    start_task = PythonOperator(
        task_id='mark_start',
        python_callable=mark_start,
        provide_context=True,
    )
    get_token = PythonOperator(
        task_id='get_token',
        python_callable=get_token_sianet,
        provide_context=True,
        retries=5,
        retry_delay=timedelta(minutes=5),
    )
    get_data_line = PythonOperator(
        task_id='get_data_line',
        python_callable=get_data_line_sianet,
        provide_context=True,
        retries=5,
        retry_delay=timedelta(minutes=5),
    )

    insert_line = PythonOperator(
        task_id='insert_line_data',
        python_callable=insert_line_data,
        provide_context=True,
        op_kwargs={'linha_data_json': "{{ ti.xcom_pull(task_ids='get_data_line') }}"},
        retries=5,
        retry_delay=timedelta(minutes=5),
    )

    end = EmptyOperator(task_id='end')

    start >> start_task >> get_token >> get_data_line >> insert_line >> end


dag = pipeline_hp_sianet()
