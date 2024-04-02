# from datetime import datetime, timedelta
# from requests import request
# import requests_mock
# from airflow.dags.utils.connections import get_token_bearer, get_data
#
#
# def test_get_token_bearer():
#     with requests_mock.Mocker() as m:
#         url_auth = "https://identity.us.mixtelematics.com/core/connect/token"
#         credentials = {"usuario": "seu_usuario", "senha": "sua_senha"}
#         token_bearer = "token_mockado"
#         m.post(url_auth, json={"access_token": token_bearer})
#
#         token = get_token_bearer(url_auth, credentials)
#         assert token == token_bearer
#
#
# def test_get_datetime():
#     # Definir URL de autenticação e credenciais de teste
#     url_auth_test = "https://example.com/auth"
#     credentials_test = {"usuario": "teste", "senha": "1234"}
#     yesterday = datetime.now() + timedelta(days=1)  # Ajuste para obter o dia anterior, não o próximo
#     yesterday_formatted = yesterday.strftime("%Y%m%d")
#     result = get_data(url_auth_test, credentials_test)
#
#     assert result == yesterday_formatted
#
#
# def test_get_geodata(url_base):
#     url_base = "https://integrate.us.mixtelematics.com/api/geodata/assetmovements/-8537705117441354628/"
#     token = get_token_bearer()
#     payload = {}
#     headers = {
#         'Authorization': f'Bearer {token}'
#     }
#     date = get_data()
#     url_base = url_base + date + "000000" + "/" + date + "235959"
#     response = request("GET", url_base, headers=headers, data=payload)
#     response
#
#
# def test_create_test():
#     # Setup: Preparar quaisquer dados de teste ou configurações necessárias (se houver)
#     a = 5
#     b = 3
#     # Ação: Chamar a funcionalidade sendo testada
#     resultado = create_test(a, b)
#     # Assertiva: Verificar se o resultado é o esperado
#     assert resultado == 8
