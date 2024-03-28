from datetime import datetime, timedelta

from requests import request


def get_token_bearer(url_auth, credentials):
    url_integrate = "https://identity.us.mixtelematics.com/core/connect/token"
    payload = ('grant_type=password'
               '&username=flavislei.costa%40hptransportes.com.br'
               '&password=flavisleicosta2024&scope'
               '=offline_access%20MiX.Integrate')
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': 'Basic bWl4YnJocHRyYW5zcG9ydGVzOmlBMTA4NmQ2aWhoUUZzV0Y='
    }
    response = request("POST", url_integrate, headers=headers, data=payload)
    return response.json()['access_token']


def get_data(url_auth, credentials):
    yesterday = datetime.now() + timedelta(days=1)
    yesterday_formatted = yesterday.strftime("%Y%m%d")

    return yesterday_formatted