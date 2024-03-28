# Instalação do ambiente
## Pré-requisitos
Todos os comandos listados abaixo devem ser executados na raiz do projeto.
A versão do python que está sendo utilizada é a 3.10


## Poetry
`poetry install` comando necessário para instalar as dependências do projeto
`poetry shell` comando necessário para ativar o ambiente virtual do projeto

## Airflow
`astro dev start` comando necessário para iniciar o ambiente do Airflow
Para executar usando o ambiente local via docker, tanto o Airbyte como o AirFlow precisam estar na mesma rede
>Explicar como criar uma rede no docker
Exemplo de código adicionando todos os containers na mesma rede:
``` 
docker network connect data_network airbyte-webapp
docker network connect data_network airbyte-worker
docker network connect data_network airbyte-server
docker network connect data_network airbyte-cron
docker network connect data_network airbyte-connector-builder-server
docker network connect data_network airbyte-api-server
docker network connect data_network airbyte-temporal
docker network connect data_network airbyte-db
docker network connect data_network airbyte-docker-proxy-1
docker network connect data_network airflow_1b8929-webserver-1
docker network connect data_network airflow_1b8929-triggerer-1
docker network connect data_network airflow_1b8929-scheduler-1
docker network connect data_network airflow_1b8929-postgres-1

```


## DBT
`dbt run` comando necessário para rodar as transformações do dbt
`dbt test` comando necessário para rodar os testes do dbt

