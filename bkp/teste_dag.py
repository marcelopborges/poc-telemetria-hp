# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.operators.empty import EmptyOperator
# from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
# from datetime import datetime, timedelta
# from airflow.models import Variable
#
# # Definição das variáveis
# api_url = Variable.get("airbyte_api_url")
# geodata_id = Variable.get("geodata_airbyte_id")
#
# # Função para calcular a data no formato ddmmyyyy e enviar via XCom
# def format_and_push_date(**kwargs):
#     ds = kwargs['ds']  # 'ds' é a data de execução do DAG no formato yyyy-mm-dd
#     formatted_date = datetime.strptime(ds, '%Y-%m-%d').strftime('%Y%m%d')
#     kwargs['ti'].xcom_push(key='formatted_date', value=formatted_date)
#     return formatted_date
#
# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2024, 3, 25),
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }
#
# with DAG('teste_with_xcom',
#          schedule_interval="@daily",
#          default_args=default_args,
#          catchup=True,
#          tags=['airbyte']) as dag:
#
#     start = EmptyOperator(task_id='start')
#
#     # PythonOperator para calcular e enviar a data formatada
#     format_date = PythonOperator(
#         task_id='format_date',
#         python_callable=format_and_push_date,
#         provide_context=True,
#
#     )
#
#     # Operador para iniciar a sincronização no Airbyte
#     load_geodata_raw = AirbyteTriggerSyncOperator(
#         task_id='load_geodata_raw',
#         airbyte_conn_id='airbyte',
#         connection_id=geodata_id,
#         # Aqui você usará o Jinja Template para puxar a data formatada do XCom
#         start_date="{{ ti.xcom_pull(task_ids='format_date', key='formatted_date') }}"
#
#     )
#
#     end = EmptyOperator(task_id='end')
#
#     start >> format_date >> load_geodata_raw >> end
