# from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
# from airflow.operators.python import PythonOperator
# from airflow.decorators import dag
# from airflow.operators.empty import EmptyOperator
# from datetime import datetime, timedelta
# from airflow.models import Variable
# import requests
#
# api_url_airbyte = Variable.get("airbyte_api_url")
# geodata_id = Variable.get("geodata_airbyte_id")
#
# def update_airbyte_connection_url():
#     # Calcula as datas necessárias para a URL
#     yesterday = datetime.now() - timedelta(days=1)
#     start_date = yesterday.strftime('%Y%m%d') + '000000'
#     end_date = yesterday.strftime('%Y%m%d') + '235959'
#
#     # Formata a nova URL
#     new_url = f"https://integrate.us.mixtelematics.com/api/geodata/assetmovements/-8537705117441354628/{start_date}/{end_date}"
#     # Prepara a requisição para atualizar a conexão no Airbyte
#     update_payload = {
#         "connectionId": geodata_id,
#         "configuration": {
#             "url": new_url
#         }
#     }
#
#     # Envia a requisição para a API do Airbyte
#     response = requests.post(f"http://localhost:8006/v1/connections/update", json=update_payload)
#     if not response.ok:
#         raise Exception(f"Failed to update Airbyte connection: {response.text}")
#
# @dag(start_date=datetime(2024, 3, 25), schedule_interval=None, catchup=False, tags=['airbyte'])
# def teste2():
#     start = EmptyOperator(task_id='start')
#
#     update_url = PythonOperator(
#         task_id='update_airbyte_url',
#         python_callable=update_airbyte_connection_url,
#     )
#
#     load_geodata_raw = AirbyteTriggerSyncOperator(
#         task_id='load_geodata_raw',
#         airbyte_conn_id='airbyte_default',
#         connection_id=geodata_id,
#     )
#
#     start >> update_url >> load_geodata_raw
#
# dag = teste2()
