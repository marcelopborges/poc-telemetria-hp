# Instalação do ambiente
## Pré-requisitos
Todos os comandos listados abaixo devem ser executados na raiz do projeto.
A versão do python que está sendo utilizada é a 3.10


## Poetry
`poetry install` comando necessário para instalar as dependências do projeto
`poetry shell` comando necessário para ativar o ambiente virtual do projeto

## Airflow
`astro dev start` comando necessário para iniciar o ambiente do Airflow
>Use o site https://crontab.guru/ para gerar a expressão cron

## DBT
`dbt run` comando necessário para rodar as transformações do dbt
`dbt test` comando necessário para rodar os testes do dbt

