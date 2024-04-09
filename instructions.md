# Configuração do ambiente
## Pré-requisitos
Todos os comandos listados abaixo devem ser executados na raiz do projeto.
A versão do python que está sendo utilizada é a 3.10, postgres 16.2 (docker) e Airflow (2.8.3+astro.1)
O sistema operacional utilizado foi Ubuntu 23.10

## Postgres
Acessar a pasta postgres na raiz do projeto e executar o docker-compose ``` docker-compose up ```


## Poetry
- `poetry install` comando necessário para instalar as dependências do projeto.
- `poetry shell` comando necessário para ativar o ambiente virtual do projeto.
 
_**Para maiores informações, leia a documentação: https://python-poetry.org/docs/**_


## Airflow
- Comando necessário para iniciar o ambiente do Airflow `astro dev start`
Para executar usando o ambiente local via docker, tanto o Airbyte como o AirFlow precisam estar na mesma rede
- É recomendável criar uma rede nova e inserir todos os serviços nela, abaixo um exemplo de como criar uma rede chamada
data_network

- Para criar uma rede no ambiente docker digite:
```docker network create data_network```
- Liste os nomes de todos os containers dos sistemas Airflow e Postgres ``` docker ps -a```
- Em seguida adicione a rede criada aos containers desejados:
``` 
docker network connect data_network airflow_1b8929-webserver-1
docker network connect data_network airflow_1b8929-triggerer-1
docker network connect data_network airflow_1b8929-scheduler-1
docker network connect data_network airflow_1b8929-postgres-1
```

**_Para maiores informações, leia a documentação do Airflow: https://airflow.apache.org/docs/_**
# Preparação do ambiente
## Conexão com o Banco de dados
Os dados de conexão com o banco de dados não está hard coded e é gerenciado pelo Airflow, garantir que a variável de conexão
(connection) seja ```postgres_con_id```.
## Váriaveis de autênticação 
Nenhuma variável de autenticação ou que possa colocar em risco o ambiente está explicito no código (hard coded),
neste caso, utilizando as váriaveis disponíveis no Airflow que gera uma camada de segurança extra, com criptografia
e restrição de acesso

Segue os nomes das variáveis que deve ser criadas e preenchidas no Airflow:
### pipeline-hp-dados
- url_api_sianet: url da api de requisição de token do sianet
- auth_token_basic_sianet: Token criptografado da autenticação Basic64 para a requisição do token bearer.
- url_api_sianet_trajeto_ocioso_emp2: url da api do endpoint trajetoOcioso com somente o parametro emp = 2 preenchido.

### pipeline-hp-telemetrics
- url_identity: url para obtenção do token bearer
- username_access_identity_mix: usuário com permissão de acesso à API
- password_access_identity_mix: password do usuário com permissão de acesso à API 
- auth_token_basic_mix_identity: Token criptografado da autenticação Basic64 para a requisição do token bearer.
- id_hp_mix: ID da HP dentro da plataforma da Mix

# Disclamer
Esta pipeline é uma POC para validação do serviço prestado pela Mix Telematics e verificar as possibilidades da utilização 
da telemetria pela HP Transportes. Vale salientar, que este projeto ainda precisa de criação de testes unitários, 
utilização de um DW e ter uma camada para transformação de dados mais eficiente por exemplo DBT. Portanto, não é recomendável
sua utilização em ambiente de produção.

