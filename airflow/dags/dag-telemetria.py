from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.models import Variable
import pendulum

local_tz = pendulum.timezone("America/Sao_Paulo")
start_date = datetime(2024, 3, 19, tzinfo=local_tz)
email_list = Variable.get('var_key_adm').split(',')

default_args = {
    'owner': 'hp',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5), # As tentativas serão acrescidas de 5 minutos, ou seja, primeira tentativa após 5 minutos, segunda após 10 minutos e assim por diante. No total serão 30 minutos.
    'start_date': start_date,
}

@dag(
    default_args=default_args,
    schedule_interval='0 8 * * *',
    tags=['hp', 'telemetria', 'gds','elt', 'prod'],
    catchup=False,
)
def telemetria_dag():
    @task()
    def extract():
        print('Extracting data')

    @task()
    def load():
        print('Loading data')

    @task()
    def transform():
        print('Transforming data')

    extract() >> load() >> transform()

# Criar uma instância da DAG
dag_instance = telemetria_dag()
